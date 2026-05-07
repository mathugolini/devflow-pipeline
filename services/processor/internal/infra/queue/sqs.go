// Package queue contains the SQS adapters: a Consumer that long-polls
// raw events and a Publisher that writes processed events.
package queue

import (
	"context"
	"encoding/json"
	"fmt"

	awsconfig "github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/service/sqs"
	"github.com/aws/aws-sdk-go-v2/service/sqs/types"

	"github.com/mathugolini/devflow-pipeline/services/processor/internal/domain"
)

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
// receipt handle the worker uses to acknowledge it.
type Message struct {
	Event         domain.RawEvent
	ReceiptHandle string
	MessageID     string
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
func (c *Consumer) Receive(ctx context.Context) (msgs []Message, parseFailures []types.Message, err error) {
	out, err := c.api.ReceiveMessage(ctx, &sqs.ReceiveMessageInput{
		QueueUrl:            &c.queueURL,
		MaxNumberOfMessages: c.maxMessages,
		WaitTimeSeconds:     c.waitTimeSeconds,
	})
	if err != nil {
		return nil, nil, fmt.Errorf("receive message: %w", err)
	}
	for _, m := range out.Messages {
		var ev domain.RawEvent
		if err := json.Unmarshal([]byte(*m.Body), &ev); err != nil {
			parseFailures = append(parseFailures, m)
			continue
		}
		msgs = append(msgs, Message{
			Event:         ev,
			ReceiptHandle: *m.ReceiptHandle,
			MessageID:     *m.MessageId,
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

func (p *Publisher) Publish(ctx context.Context, event domain.ProcessedEvent) error {
	body, err := json.Marshal(event)
	if err != nil {
		return fmt.Errorf("marshal processed event: %w", err)
	}
	bodyStr := string(body)
	_, err = p.api.SendMessage(ctx, &sqs.SendMessageInput{
		QueueUrl:    &p.queueURL,
		MessageBody: &bodyStr,
	})
	if err != nil {
		return fmt.Errorf("send message: %w", err)
	}
	return nil
}

func ptrInt32(v int32) *int32 { return &v }
