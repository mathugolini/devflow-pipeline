SHELL := /bin/bash
ENDPOINT ?= http://localhost:4566
AWS := aws --endpoint-url=$(ENDPOINT) --region us-east-1
export AWS_ACCESS_KEY_ID ?= test
export AWS_SECRET_ACCESS_KEY ?= test

.PHONY: help up up-infra down logs ps verify-infra test-processor build-processor send-test clean

help:
	@grep -E '^[a-zA-Z_-]+:.*?##' $(MAKEFILE_LIST) | awk 'BEGIN{FS=":.*?##"};{printf "  %-18s %s\n", $$1, $$2}'

up-infra: ## Start LocalStack and provision SQS/DynamoDB
	docker compose up -d localstack
	docker compose run --rm aws-init

up: up-infra ## Start everything (infra + processor)
	docker compose up -d --build processor

down: ## Stop and remove containers
	docker compose down -v

logs: ## Tail LocalStack logs
	docker compose logs -f localstack

ps: ## Show running containers
	docker compose ps

verify-infra: ## Sanity-check that queues and tables exist
	@echo "==> SQS queues:" && $(AWS) sqs list-queues
	@echo "==> DynamoDB tables:" && $(AWS) dynamodb list-tables
	@echo "==> raw-events redrive policy:" && \
		$(AWS) sqs get-queue-attributes \
			--queue-url $$($(AWS) sqs get-queue-url --queue-name raw-events --query QueueUrl --output text) \
			--attribute-names RedrivePolicy VisibilityTimeout

test-processor: ## Run unit tests for the processor service
	cd services/processor && go test ./...

build-processor: ## Build the processor binary locally
	cd services/processor && go build -o ../../bin/processor ./cmd/processor

send-test: ## Send a sample valid event to raw-events
	@RAW_URL=$$($(AWS) sqs get-queue-url --queue-name raw-events --query QueueUrl --output text); \
	BODY='{"event_id":"550e8400-e29b-41d4-a716-446655440000","developer_id":"dev-1","metric_type":"commits","value":3,"repository":"org/repo","timestamp":"2026-04-15T10:30:00Z"}'; \
	$(AWS) sqs send-message --queue-url $$RAW_URL --message-body "$$BODY"; \
	echo "==> processed-events count after 5s..."; sleep 5; \
	$(AWS) sqs get-queue-attributes \
	  --queue-url $$($(AWS) sqs get-queue-url --queue-name processed-events --query QueueUrl --output text) \
	  --attribute-names ApproximateNumberOfMessages

clean: down ## Tear down everything
	rm -rf .localstack volume bin
