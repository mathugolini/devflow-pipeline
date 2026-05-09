// Package repository implements the aggregator's persistence ports
// against DynamoDB.
//
// The repo exposes two operations matching the use case's split:
//   - SaveEvent: PutItem on the events table with a conditional
//     attribute_not_exists(event_id). Duplicates surface as
//     domain.AlreadyProcessedError and the use case treats them as a
//     successful (deletable) outcome.
//   - UpdateSummary: atomic UpdateItem on developer_summary using ADD
//     for the counter deltas.
//
// Trade-off vs. a single TransactWriteItems: with two separate calls,
// if SaveEvent succeeds and UpdateSummary fails, the SQS message is
// not deleted and gets redelivered. On the retry, SaveEvent returns
// AlreadyProcessed and the summary update is skipped — that single
// event's contribution is lost. Acceptable for this exercise; in
// production we would either (a) use TransactWriteItems, or (b) mark
// summary_applied=true on the event row only after UpdateSummary
// succeeds and re-drive the update on retries when that flag is false.
package repository

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	awsconfig "github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/feature/dynamodb/attributevalue"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
	ddbtypes "github.com/aws/aws-sdk-go-v2/service/dynamodb/types"

	"github.com/mathugolini/devflow-pipeline/services/aggregator/internal/domain"
)

// DynamoDB is the concrete repository. It implements both
// usecase.EventRepository and usecase.SummaryRepository.
type DynamoDB struct {
	api          *dynamodb.Client
	eventsTable  string
	summaryTable string
	gsiName      string
	clock        func() time.Time
}

// NewClient builds a DynamoDB client respecting an optional endpoint
// override (LocalStack).
func NewClient(ctx context.Context, region, endpointURL string) (*dynamodb.Client, error) {
	cfg, err := awsconfig.LoadDefaultConfig(ctx, awsconfig.WithRegion(region))
	if err != nil {
		return nil, fmt.Errorf("load aws config: %w", err)
	}
	api := dynamodb.NewFromConfig(cfg, func(o *dynamodb.Options) {
		if endpointURL != "" {
			o.BaseEndpoint = &endpointURL
		}
	})
	return api, nil
}

func New(api *dynamodb.Client, eventsTable, summaryTable string) *DynamoDB {
	return &DynamoDB{
		api:          api,
		eventsTable:  eventsTable,
		summaryTable: summaryTable,
		gsiName:      "developer_id-index",
		clock:        func() time.Time { return time.Now().UTC() },
	}
}

// SaveEvent persists the event row with a conditional write. Returns
// *domain.AlreadyProcessedError when the event_id was already stored
// (idempotent retry path).
func (r *DynamoDB) SaveEvent(ctx context.Context, evt domain.ProcessedEvent) error {
	rec := evt.ToRecord()
	item, err := attributevalue.MarshalMap(rec)
	if err != nil {
		return fmt.Errorf("marshal event: %w", err)
	}
	_, err = r.api.PutItem(ctx, &dynamodb.PutItemInput{
		TableName:           aws.String(r.eventsTable),
		Item:                item,
		ConditionExpression: aws.String("attribute_not_exists(event_id)"),
	})
	if err != nil {
		var ccf *ddbtypes.ConditionalCheckFailedException
		if errors.As(err, &ccf) {
			return &domain.AlreadyProcessedError{EventID: rec.EventID}
		}
		return fmt.Errorf("put event: %w", err)
	}
	return nil
}

// UpdateSummary applies the per-event ADD deltas to the developer's
// summary row in a single UpdateItem.
func (r *DynamoDB) UpdateSummary(ctx context.Context, evt domain.ProcessedEvent) error {
	inc := domain.IncrementsFor(evt, r.clock())

	now := inc.Now.UTC().Format(time.RFC3339Nano)
	ts := inc.EventTimestamp.UTC().Format(time.RFC3339Nano)

	updateExpr := "ADD total_commits :inc_c, total_pull_requests :inc_pr, " +
		"total_review_time_minutes :inc_rtm, review_time_count :inc_rtc, " +
		"events_processed :one " +
		// last_activity is set unconditionally to the event's timestamp.
		// Trade-off: under heavy concurrent out-of-order writes this can
		// momentarily go backwards. Acceptable for the demo.
		"SET last_activity = :ts, updated_at = :now"

	exprValues := map[string]ddbtypes.AttributeValue{
		":inc_c":   &ddbtypes.AttributeValueMemberN{Value: itoa(inc.Commits)},
		":inc_pr":  &ddbtypes.AttributeValueMemberN{Value: itoa(inc.PullRequests)},
		":inc_rtm": &ddbtypes.AttributeValueMemberN{Value: ftoa(inc.ReviewTimeMinutes)},
		":inc_rtc": &ddbtypes.AttributeValueMemberN{Value: itoa(inc.ReviewTimeCount)},
		":one":     &ddbtypes.AttributeValueMemberN{Value: "1"},
		":ts":      &ddbtypes.AttributeValueMemberS{Value: ts},
		":now":     &ddbtypes.AttributeValueMemberS{Value: now},
	}

	_, err := r.api.UpdateItem(ctx, &dynamodb.UpdateItemInput{
		TableName: aws.String(r.summaryTable),
		Key: map[string]ddbtypes.AttributeValue{
			"developer_id": &ddbtypes.AttributeValueMemberS{Value: evt.DeveloperID},
		},
		UpdateExpression:          aws.String(updateExpr),
		ExpressionAttributeValues: exprValues,
	})
	if err != nil {
		return fmt.Errorf("update summary: %w", err)
	}
	return nil
}

// ListByDeveloper queries the developer_id GSI on the events table.
func (r *DynamoDB) ListByDeveloper(ctx context.Context, developerID string) ([]domain.EventRecord, error) {
	out, err := r.api.Query(ctx, &dynamodb.QueryInput{
		TableName:              aws.String(r.eventsTable),
		IndexName:              aws.String(r.gsiName),
		KeyConditionExpression: aws.String("developer_id = :d"),
		ExpressionAttributeValues: map[string]ddbtypes.AttributeValue{
			":d": &ddbtypes.AttributeValueMemberS{Value: developerID},
		},
	})
	if err != nil {
		return nil, fmt.Errorf("query events by developer: %w", err)
	}
	recs := make([]domain.EventRecord, 0, len(out.Items))
	for _, item := range out.Items {
		var rec domain.EventRecord
		if err := attributevalue.UnmarshalMap(item, &rec); err != nil {
			return nil, fmt.Errorf("unmarshal event: %w", err)
		}
		recs = append(recs, rec)
	}
	return recs, nil
}

// GetSummary returns the summary row, found=false if the row does not
// exist yet.
func (r *DynamoDB) GetSummary(ctx context.Context, developerID string) (domain.DeveloperSummary, bool, error) {
	out, err := r.api.GetItem(ctx, &dynamodb.GetItemInput{
		TableName: aws.String(r.summaryTable),
		Key: map[string]ddbtypes.AttributeValue{
			"developer_id": &ddbtypes.AttributeValueMemberS{Value: developerID},
		},
	})
	if err != nil {
		return domain.DeveloperSummary{}, false, fmt.Errorf("get summary: %w", err)
	}
	if len(out.Item) == 0 {
		return domain.DeveloperSummary{}, false, nil
	}
	var s domain.DeveloperSummary
	if err := attributevalue.UnmarshalMap(out.Item, &s); err != nil {
		return domain.DeveloperSummary{}, false, fmt.Errorf("unmarshal summary: %w", err)
	}
	return s, true, nil
}

// HealthEvents returns nil if the events table is reachable.
func (r *DynamoDB) HealthEvents(ctx context.Context) error {
	_, err := r.api.DescribeTable(ctx, &dynamodb.DescribeTableInput{
		TableName: aws.String(r.eventsTable),
	})
	return err
}

func itoa(v int64) string   { return fmt.Sprintf("%d", v) }
func ftoa(v float64) string { return fmt.Sprintf("%g", v) }
