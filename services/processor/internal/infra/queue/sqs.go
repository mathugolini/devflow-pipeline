// Package queue contains the SQS adapters: a Consumer that long-polls
// raw events and a Publisher that writes processed events.
package queue

import (
	"context"
	"encoding/json"
	"fmt"
	"strings"

	awsconfig "github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/service/sqs"
	"github.com/aws/aws-sdk-go-v2/service/sqs/types"
	"go.opentelemetry.io/contrib/instrumentation/github.com/aws/aws-sdk-go-v2/otelaws"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/trace"

	"github.com/mathugolini/devflow-pipeline/services/processor/internal/domain"
)

const tracerName = "github.com/mathugolini/devflow-pipeline/services/processor"

// Client wraps an AWS SQS client and resolves queue URLs.
type Client struct {
	api *sqs.Client
}

// NewClient builds an SQS client. If endpointURL is non-empty, it is used
// as the LocalStack/AWS endpoint override.
func NewClient(ctx context.Context, region, endpointURL string) (*Client, error) {
	cfg, err := awsconfig.LoadDefaultConfig(ctx, awsconfig.WithRegion(region))
	if err != nil {
		return nil, fmt.Errorf("load aws config: %w", err)
	}
	// otelaws appends per-AWS-call client-side spans (sqs.SendMessage,
	// sqs.ReceiveMessage, ...). Cheap and gives us latency breakdown
	// for free, on top of the manual producer/consumer spans below.
	otelaws.AppendMiddlewares(&cfg.APIOptions)
	api := sqs.NewFromConfig(cfg, func(o *sqs.Options) {
		if endpointURL != "" {
			o.BaseEndpoint = &endpointURL
		}
	})
	return &Client{api: api}, nil
}

// QueueURL resolves a queue name to its URL.
func (c *Client) QueueURL(ctx context.Context, name string) (string, error) {
	out, err := c.api.GetQueueUrl(ctx, &sqs.GetQueueUrlInput{QueueName: &name})
	if err != nil {
		return "", fmt.Errorf("get queue url %q: %w", name, err)
	}
	return *out.QueueUrl, nil
}

// Ping does a cheap call so we can fail fast at startup.
func (c *Client) Ping(ctx context.Context) error {
	_, err := c.api.ListQueues(ctx, &sqs.ListQueuesInput{MaxResults: ptrInt32(1)})
	return err
}

// Message is a raw event delivered by the consumer along with the SQS
// receipt handle the worker uses to acknowledge it. Context carries
// the upstream trace context extracted from MessageAttributes.
type Message struct {
	Event         domain.RawEvent
	ReceiptHandle string
	MessageID     string
	// Context propagates the upstream trace context. May be
	// context.Background() when no trace headers were present
	// (e.g. messages produced by the seed script).
	Context context.Context
}

// Consumer long-polls a queue and emits domain.RawEvent values.
type Consumer struct {
	api             *sqs.Client
	queueURL        string
	waitTimeSeconds int32
	maxMessages     int32
}

func NewConsumer(c *Client, queueURL string, waitTimeSeconds int32, maxMessages int32) *Consumer {
	if maxMessages <= 0 || maxMessages > 10 {
		maxMessages = 10
	}
	return &Consumer{api: c.api, queueURL: queueURL, waitTimeSeconds: waitTimeSeconds, maxMessages: maxMessages}
}

// Receive performs a single long-poll. Returns Messages whose body parsed
// successfully. Bodies that fail to parse are returned as parseFailures so
// the caller can decide what to do (currently: leave them on the queue
// → SQS DLQ after maxReceiveCount).
//
// MessageAttributeNames=["All"] is required so SQS returns the
// W3C trace headers the producer set; SQS strips them by default.
func (c *Consumer) Receive(ctx context.Context) (msgs []Message, parseFailures []types.Message, err error) {
	out, err := c.api.ReceiveMessage(ctx, &sqs.ReceiveMessageInput{
		QueueUrl:              &c.queueURL,
		MaxNumberOfMessages:   c.maxMessages,
		WaitTimeSeconds:       c.waitTimeSeconds,
		MessageAttributeNames: []string{"All"},
	})
	if err != nil {
		return nil, nil, fmt.Errorf("receive message: %w", err)
	}
	prop := otel.GetTextMapPropagator()
	for _, m := range out.Messages {
		var ev domain.RawEvent
		dec := json.NewDecoder(strings.NewReader(*m.Body))
		// Strict mode: reject unknown fields. Catches schema drift between
		// the producer's contract and the consumer's struct early instead
		// of silently dropping data.
		dec.DisallowUnknownFields()
		if err := dec.Decode(&ev); err != nil {
			parseFailures = append(parseFailures, m)
			continue
		}
		msgCtx := prop.Extract(context.Background(), sqsAttrCarrier{attrs: m.MessageAttributes})
		msgs = append(msgs, Message{
			Event:         ev,
			ReceiptHandle: *m.ReceiptHandle,
			MessageID:     *m.MessageId,
			Context:       msgCtx,
		})
	}
	return msgs, parseFailures, nil
}

// Delete acknowledges a successfully processed message.
func (c *Consumer) Delete(ctx context.Context, receiptHandle string) error {
	_, err := c.api.DeleteMessage(ctx, &sqs.DeleteMessageInput{
		QueueUrl:      &c.queueURL,
		ReceiptHandle: &receiptHandle,
	})
	if err != nil {
		return fmt.Errorf("delete message: %w", err)
	}
	return nil
}

// Publisher writes processed events to the outbound queue.
type Publisher struct {
	api      *sqs.Client
	queueURL string
}

func NewPublisher(c *Client, queueURL string) *Publisher {
	return &Publisher{api: c.api, queueURL: queueURL}
}

// Publish opens a producer span, injects the W3C trace headers into
// SQS MessageAttributes, and sends the body. The span context becomes
// the parent of the next service's consumer span.
func (p *Publisher) Publish(ctx context.Context, event domain.ProcessedEvent) error {
	ctx, span := otel.Tracer(tracerName).Start(ctx, "sqs.send processed-events",
		trace.WithSpanKind(trace.SpanKindProducer),
		trace.WithAttributes(
			attribute.String("messaging.system", "aws_sqs"),
			attribute.String("messaging.destination.name", "processed-events"),
			attribute.String("event_id", event.EventID),
		),
	)
	defer span.End()

	body, err := json.Marshal(event)
	if err != nil {
		span.RecordError(err)
		return fmt.Errorf("marshal processed event: %w", err)
	}
	bodyStr := string(body)

	attrs := make(map[string]types.MessageAttributeValue, 2)
	otel.GetTextMapPropagator().Inject(ctx, sqsAttrCarrier{attrs: attrs})

	_, err = p.api.SendMessage(ctx, &sqs.SendMessageInput{
		QueueUrl:          &p.queueURL,
		MessageBody:       &bodyStr,
		MessageAttributes: attrs,
	})
	if err != nil {
		span.RecordError(err)
		return fmt.Errorf("send message: %w", err)
	}
	return nil
}

func ptrInt32(v int32) *int32 { return &v }
