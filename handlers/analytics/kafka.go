package analytics

import (
	"context"
	"crypto/tls"

	"github.com/golang/glog"
	"github.com/livepeer/catalyst-api/metrics"
	"github.com/segmentio/kafka-go"
	"github.com/segmentio/kafka-go/sasl/plain"
)

func sendWithRetries(writer *kafka.Writer, msgs []kafka.Message) {
	// We retry sending messages to Kafka in case of a failure
	// We don't use any backoff, because the number of events are filling up very quickly, so in case of a failure
	// it's better to lose events than fill up the memory and crash the whole catalyst-api
	kafkaWriteRetries := 3
	var err error
	for i := 0; i < kafkaWriteRetries; i++ {
		err = writer.WriteMessages(context.Background(), msgs...)
		if err == nil {
			return
		} else {
			glog.Warningf("error while sending analytics log to Kafka, retrying, try=%d, err=%v", i, err)
		}
	}
	metrics.Metrics.AnalyticsMetrics.LogProcessorWriteErrors.Inc()
	glog.Errorf("error while sending events to Kafka, the events are lost, err=%d", err)
}

func logWriteMetrics(writer *kafka.Writer) {
	stats := writer.Stats()
	metrics.Metrics.AnalyticsMetrics.KafkaWriteErrors.Add(float64(stats.Errors))
	metrics.Metrics.AnalyticsMetrics.KafkaWriteMessages.Add(float64(stats.Messages))
	metrics.Metrics.AnalyticsMetrics.KafkaWriteAvgTime.Observe(stats.WriteTime.Avg.Seconds())
	metrics.Metrics.AnalyticsMetrics.KafkaWriteRetries.Add(float64(stats.Retries))
}

func newWriter(bootstrapServers, user, password, topic string) *kafka.Writer {
	dialer := &kafka.Dialer{
		Timeout: kafkaRequestTimeout,
		SASLMechanism: plain.Mechanism{
			Username: user,
			Password: password,
		},
		DualStack: true,
		TLS: &tls.Config{
			MinVersion: tls.VersionTLS12,
		},
	}

	// Create a new Kafka writer
	return kafka.NewWriter(kafka.WriterConfig{
		Brokers:  []string{bootstrapServers},
		Topic:    topic,
		Balancer: kafka.CRC32Balancer{},
		Dialer:   dialer,
	})
}
