SHELL := /bin/bash
ENDPOINT ?= http://localhost:4566
AWS := aws --endpoint-url=$(ENDPOINT) --region us-east-1
export AWS_ACCESS_KEY_ID ?= test
export AWS_SECRET_ACCESS_KEY ?= test

.PHONY: help up down logs ps init verify-infra clean

help:
	@grep -E '^[a-zA-Z_-]+:.*?##' $(MAKEFILE_LIST) | awk 'BEGIN{FS=":.*?##"};{printf "  %-18s %s\n", $$1, $$2}'

up: ## Start LocalStack and provision SQS/DynamoDB
	docker compose up -d localstack
	docker compose run --rm aws-init

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

clean: down ## Tear down everything
	rm -rf .localstack volume
