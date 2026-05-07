SHELL := /bin/bash

# Run the AWS CLI either locally (if installed) or via the project's docker
# network using the same image as aws-init. This keeps the project free of
# local AWS CLI as a hard requirement.
AWS_LOCAL := $(shell command -v aws 2>/dev/null)
ifdef AWS_LOCAL
AWS := AWS_ACCESS_KEY_ID=test AWS_SECRET_ACCESS_KEY=test aws --endpoint-url=http://localhost:4566 --region us-east-1
else
AWS := docker run --rm --network devflow-pipeline_devflow \
  -e AWS_ACCESS_KEY_ID=test -e AWS_SECRET_ACCESS_KEY=test \
  amazon/aws-cli:2.17.0 --endpoint-url=http://localstack:4566 --region us-east-1
endif

.PHONY: help up up-infra down logs ps verify-infra test-processor build-processor send-test send-bad clean

help:
	@grep -E '^[a-zA-Z_-]+:.*?##' $(MAKEFILE_LIST) | awk 'BEGIN{FS=":.*?##"};{printf "  %-18s %s\n", $$1, $$2}'

up-infra: ## Start LocalStack and provision SQS/DynamoDB
	docker compose up -d localstack
	docker compose run --rm aws-init

up: up-infra ## Start everything (infra + processor)
	docker compose up -d --build processor

down: ## Stop and remove containers
	docker compose down -v

logs: ## Tail processor logs
	docker compose logs -f processor

logs-localstack: ## Tail LocalStack logs
	docker compose logs -f localstack

ps: ## Show running containers
	docker compose ps

verify-infra: ## Sanity-check that queues and tables exist
	@echo "==> SQS queues:" && $(AWS) sqs list-queues
	@echo "==> DynamoDB tables:" && $(AWS) dynamodb list-tables

test-processor: ## Run unit tests for the processor service
	cd services/processor && go test ./...

build-processor: ## Build the processor binary locally
	cd services/processor && go build -o ../../bin/processor ./cmd/processor

send-test: ## Send a valid event and check the processed-events count
	@TS=$$(date -u +%Y-%m-%dT%H:%M:%SZ); \
	UUID=$$(uuidgen | tr '[:upper:]' '[:lower:]'); \
	BODY=$$(printf '{"event_id":"%s","developer_id":"dev-1","metric_type":"commits","value":3,"repository":"org/repo","timestamp":"%s"}' $$UUID $$TS); \
	echo "==> sending: $$BODY"; \
	RAW_URL=$$($(AWS) sqs get-queue-url --queue-name raw-events --query QueueUrl --output text | tr -d '\r'); \
	$(AWS) sqs send-message --queue-url $$RAW_URL --message-body "$$BODY" >/dev/null; \
	echo "==> waiting 3s for processor..."; sleep 3; \
	OUT_URL=$$($(AWS) sqs get-queue-url --queue-name processed-events --query QueueUrl --output text | tr -d '\r'); \
	echo "==> processed-events attributes:"; \
	$(AWS) sqs get-queue-attributes --queue-url $$OUT_URL \
	  --attribute-names ApproximateNumberOfMessages ApproximateNumberOfMessagesNotVisible

send-bad: ## Send an invalid event (will end up in raw-events-dlq after 3 attempts)
	@BODY='{"event_id":"not-a-uuid","developer_id":"","metric_type":"deploys","value":-1,"timestamp":"2099-01-01T00:00:00Z"}'; \
	echo "==> sending invalid: $$BODY"; \
	RAW_URL=$$($(AWS) sqs get-queue-url --queue-name raw-events --query QueueUrl --output text | tr -d '\r'); \
	$(AWS) sqs send-message --queue-url $$RAW_URL --message-body "$$BODY" >/dev/null; \
	echo "==> done. Watch 'make logs' — message will redrive to raw-events-dlq after 3 attempts."

clean: down ## Tear down everything
	rm -rf .localstack volume bin
