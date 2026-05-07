// Package repository implements the aggregator's persistence port
// against DynamoDB. The write path uses TransactWriteItems so the
// events.PutItem and developer_summary.UpdateItem either both apply
// or neither does — see D5.
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

// DynamoDB is the concrete Repository.
type DynamoDB struct {
	api          *dynamodb.Client
	eventsTable  string
	summaryTable string
	gsiName      string
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
	}
}

// PersistAndAggregate writes the event row and the summary update in
// a single DynamoDB transaction. If the event already existed, it
// returns nil (idempotent retry).
func (r *DynamoDB) PersistAndAggregate(ctx context.Context, rec domain.EventRecord, inc domain.SummaryIncrements) error {
	item, err := attributevalue.MarshalMap(rec)
	if err != nil {
		return fmt.Errorf("marshal event: %w", err)
	}

	now := inc.Now.UTC().Format(time.RFC3339Nano)
	ts := inc.EventTimestamp.UTC().Format(time.RFC3339Nano)

	updateExpr := "ADD total_commits :inc_c, total_pull_requests :inc_pr, " +
		"total_review_time_minutes :inc_rtm, review_time_count :inc_rtc, " +
		"events_processed :one " +
		// last_activity is set unconditionally to the event's timestamp.
		// Trade-off: under heavy concurrent out-of-order writes this can
		// momentarily go backwards. Acceptable for the demo; in prod we'd
		// use a conditional expression `:ts > last_activity` outside the
		// transaction or store last_activity_epoch + use ADD with max.
		"SET last_activity = :ts, updated_at = :now"

	exprValues := map[string]ddbtypes.AttributeValue{
		":inc_c":   &ddbtypes.AttributeValueMemberN{Value: itoa(inc.Commits)},
		":inc_pr":  &ddbtypes.AttributeValueMemberN{Value: itoa(inc.PullRequests)},
		":inc_rtm": &ddbtypes.AttributeValueMemberN{Value: ftoa(inc.ReviewTimeMinutes)},
		":inc_rtc": &ddbtypes.AttributeValueMemberN{Value: itoa(inc.ReviewTimeCount)},
		":one":    &ddbtypes.AttributeValueMemberN{Value: "1"},
		":ts":     &ddbtypes.AttributeValueMemberS{Value: ts},
		":now":    &ddbtypes.AttributeValueMemberS{Value: now},
	}

	input := &dynamodb.TransactWriteItemsInput{
		TransactItems: []ddbtypes.TransactWriteItem{
			{
				Put: &ddbtypes.Put{
					TableName:           aws.String(r.eventsTable),
					Item:                item,
					ConditionExpression: aws.String("attribute_not_exists(event_id)"),
				},
			},
			{
				Update: &ddbtypes.Update{
					TableName: aws.String(r.summaryTable),
					Key: map[string]ddbtypes.AttributeValue{
						"developer_id": &ddbtypes.AttributeValueMemberS{Value: rec.DeveloperID},
					},
					UpdateExpression:          aws.String(updateExpr),
					ExpressionAttributeValues: exprValues,
				},
			},
		},
	}

	_, err = r.api.TransactWriteItems(ctx, input)
	if err != nil {
		var canceled *ddbtypes.TransactionCanceledException
		if errors.As(err, &canceled) {
			// CancellationReasons[0] corresponds to TransactItems[0]
			// (the conditional Put on events). If it's a
			// ConditionalCheckFailed, the event already existed →
			// idempotent retry, return nil.
			if len(canceled.CancellationReasons) > 0 {
				code := canceled.CancellationReasons[0].Code
				if code != nil && *code == "ConditionalCheckFailed" {
					return nil
				}
			}
		}
		return fmt.Errorf("transact write: %w", err)
	}
	return nil
}

// ListEventsByDeveloper queries the developer_id GSI.
func (r *DynamoDB) ListEventsByDeveloper(ctx context.Context, developerID string) ([]domain.EventRecord, error) {
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

func itoa(v int64) string  { return fmt.Sprintf("%d", v) }
func ftoa(v float64) string { return fmt.Sprintf("%g", v) }
