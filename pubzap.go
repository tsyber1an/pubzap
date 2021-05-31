package pubzap

import (
	"context"
	"fmt"
	"io"
	"log"
	"net/url"
	"path"
	"time"

	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
	"gocloud.dev/pubsub"
	_ "gocloud.dev/pubsub/gcppubsub"
	_ "gocloud.dev/pubsub/mempubsub"
)

var defaultPublishTimeout = 1 * time.Second

var schemas = []string{"mem", "gcppubsub"}

// we need to tell zap to recognize pubsub urls.
func init() {
	for _, schema := range schemas {
		if err := registerSink(schema); err != nil {
			panic(err)
		}
	}
}

func registerSink(protocol string) error {
	return zap.RegisterSink(protocol, func(u *url.URL) (zap.Sink, error) {
		topicName := path.Join(u.Host, u.Path)

		ctx := context.Background()
		topic, err := pubsub.OpenTopic(ctx, fmt.Sprintf("%s://%s", protocol, topicName))
		if err != nil {
			return nil, err
		}

		return &pubsubSink{topic: topic}, nil
	})
}

// pubsubSink is struct that satisfies zap.Sink
type pubsubSink struct {
	topic *pubsub.Topic

	zapcore.WriteSyncer
	io.Closer
}

// Close implement io.Closer.
func (zpb *pubsubSink) Close() error {
	return zpb.topic.Shutdown(context.Background())
}

// Write implement zap.Sink func Write
func (zpb *pubsubSink) Write(b []byte) (int, error) {
	go func() {
		ctx, cancel := context.WithTimeout(context.Background(), defaultPublishTimeout)
		defer cancel()

		err := zpb.topic.Send(ctx, &pubsub.Message{
			Body: b,
		})

		if err != nil {
			log.Printf("failed to send a pubsub message: %s", err)
		}
	}()

	return len(b), nil
}

// Sync implement zap.Sink func Sync. In fact, we do nothing here.
func (zpb *pubsubSink) Sync() error {
	return nil
}
