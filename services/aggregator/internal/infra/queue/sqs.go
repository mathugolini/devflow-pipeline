// Package queue is the SQS adapter for the aggregator. It only
// consumes from processed-events; there is no outbound publisher.
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

	"github.com/mathugolini/devflow-pipeline/services/aggregator/internal/domain"
)

func NewClient(ctx context.Context, region, endpointURL string) (*sqs.Client, error) {
	cfg, err := awsconfig.LoadDefaultConfig(ctx, awsconfig.WithRegion(region))
	if err != nil {
		return nil, fmt.Errorf("load aws config: %w", err)
	}
	otelaws.AppendMiddlewares(&cfg.APIOptions)
	api := sqs.NewFromConfig(cfg, func(o *sqs.Options) {
		if endpointURL != "" {
			o.BaseEndpoint = &endpointURL
		}
	})
	return api, nil
}

// Message is one received SQS message paired with the parsed event.
// Context carries the upstream trace context extracted from the SQS
// MessageAttributes (W3C traceparent set by the Processor).
type Message struct {
	Event         domain.ProcessedEvent
	ReceiptHandle string
	MessageID     string
	Context       context.Context
}

// Consumer long-polls processed-events.
type Consumer struct {
	api             *sqs.Client
	queueName       string
	queueURL        string
	waitTimeSeconds int32
	maxMessages     int32
}

func NewConsumer(api *sqs.Client, queueName, queueURL string, waitTimeSeconds, maxMessages int32) *Consumer {
	if maxMessages <= 0 || maxMessages > 10 {
		maxMessages = 10
	}
	return &Consumer{
		api:             api,
		queueName:       queueName,
		queueURL:        queueURL,
		waitTimeSeconds: waitTimeSeconds,
		maxMessages:     maxMessages,
	}
}

// ResolveQueueURL is a small helper used at boot.
func ResolveQueueURL(ctx context.Context, api *sqs.Client, name string) (string, error) {
	out, err := api.GetQueueUrl(ctx, &sqs.GetQueueUrlInput{QueueName: &name})
	if err != nil {
		return "", fmt.Errorf("get queue url %q: %w", name, err)
	}
	return *out.QueueUrl, nil
}

// Receive long-polls once. Bodies that fail to parse are returned as
// parseFailures so the caller can leave them for SQS DLQ redrive.
//
// MessageAttributeNames=["All"] is required so SQS returns the W3C
// trace headers the producer set (otherwise SQS strips them).
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
		var ev domain.ProcessedEvent
		dec := json.NewDecoder(strings.NewReader(*m.Body))
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

// Health pings SQS by resolving the configured queue URL.
func (c *Consumer) Health(ctx context.Context) error {
	_, err := c.api.GetQueueUrl(ctx, &sqs.GetQueueUrlInput{QueueName: &c.queueName})
	return err
}

