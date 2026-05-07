# Phase 3 — Aggregator

This phase introduces the second microservice in the pipeline. The aggregator
consumes the `processed-events` SQS queue, persists each event to DynamoDB, and
maintains a per-developer summary projection. It exposes a small read-only
HTTP API for both events and summaries.

```
[SQS: processed-events] → [Aggregator] → [DynamoDB: events + developer_summary] → [HTTP :8080]
```

## How D1–D6 are implemented

| ID | Spec | Code |
| --- | --- | --- |
| D1 | `schema_version: 1` on the wire | `services/processor/internal/domain/event.go`, `services/aggregator/internal/usecase/aggregate_event.go` |
| D2 | TTL 30d + GSI on developer_id | `infra/localstack/init-aws.sh` (`create_events_table`, `enable_ttl`); `services/aggregator/internal/domain/event.go` (`ToRecord` derives `expires_at`) |
| D3 | Hot partition: accept + monitor | Documented; no shard-on-write |
| D4 | Store components, compute avg in handler | `services/aggregator/internal/domain/event.go` (`AvgReviewTimeMinutes`) |
| D5 | TransactWriteItems for atomicity | `services/aggregator/internal/infra/repository/dynamodb.go` |
| D6 | Middleware-ready router, no auth | `services/aggregator/internal/infra/api/router.go` |

## Key code paths

- **Schema validation** — `usecase.AggregateEvent.Execute` returns
  `*domain.UnsupportedSchemaError` for any `schema_version != 1`. The worker
  treats this like the processor treats `*ValidationError`: do NOT delete the
  message; SQS DLQs after `maxReceiveCount`.
- **Idempotency** — `events.PutItem` carries
  `ConditionExpression: attribute_not_exists(event_id)`. On
  `TransactionCanceledException`, if `CancellationReasons[0].Code ==
  "ConditionalCheckFailed"`, the repository returns `nil` and the worker
  deletes the message. Re-deliveries are no-ops on both tables.
- **Per-developer query** — `events` has a `developer_id-index` GSI so the
  API uses `Query` rather than `Scan`.

## Limitations / explicit trade-offs

- **`last_activity` overwrite.** The `developer_summary` update unconditionally
  `SET`s `last_activity = :ts`. Under heavy out-of-order delivery this can
  briefly move the timestamp backwards. Acceptable for the demo; in prod we'd
  store an epoch field and use `ADD` with max semantics or a conditional
  expression outside the transaction.
- **Hot partition.** `developer_id` is the partition key on
  `developer_summary`. PAY_PER_REQUEST mode auto-rebalances, but a runaway
  service-account producer can still throttle. Mitigation path is sharding
  (`developer_id#shard0..N`) or moving counters to Redis with periodic flush.
- **No auth.** The HTTP API is open. The router is variadic-middleware-ready
  so adding JWT/API-key auth is a single line in `main.go`.

## End-to-end smoke test

```bash
make up           # localstack + processor + aggregator
make seed         # ~21 mixed events
sleep 5
curl -s http://localhost:8080/health
curl -s http://localhost:8080/metrics/dev-1/summary
curl -s http://localhost:8080/metrics/dev-1
```

Expected after seed:
- `dev-1` summary has `total_commits >= 11` (10 fresh + 1 from duplicate, second
  duplicate is idempotent), `review_time_count = 3`,
  `avg_review_time_minutes = 30`.
- `dev-2` summary has `total_pull_requests = 5`.
- `raw-events-dlq` holds 2 messages from the invalid events after 3 retries.
