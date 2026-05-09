package queue

import (
	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/sqs/types"
)

// sqsAttrCarrier adapts SQS MessageAttributes to the OTel
// TextMapCarrier interface so the W3C TraceContext propagator can
// inject/extract trace headers (traceparent, tracestate) without any
// glue at the call site.
//
// All values are stored as DataType=String — the only type SQS's
// propagator semantics need. The carrier is valid as long as the
// underlying map is non-nil; callers must initialize it before Set.
type sqsAttrCarrier struct {
	attrs map[string]types.MessageAttributeValue
}

func (c sqsAttrCarrier) Get(key string) string {
	v, ok := c.attrs[key]
	if !ok || v.StringValue == nil {
		return ""
	}
	return *v.StringValue
}

func (c sqsAttrCarrier) Set(key, value string) {
	if c.attrs == nil {
		return
	}
	v := value
	c.attrs[key] = types.MessageAttributeValue{
		DataType:    aws.String("String"),
		StringValue: &v,
	}
}

func (c sqsAttrCarrier) Keys() []string {
	keys := make([]string, 0, len(c.attrs))
	for k := range c.attrs {
		keys = append(keys, k)
	}
	return keys
}
