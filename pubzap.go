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

// registerSink
// url format:
// host and path is a topic name
// query:
// - publishTimeout=X where X A duration string is a possibly signed
//    sequence of decimal numbers, each with optional fraction and a unit suffix,
//    such as "300ms", "-1.5h" or "2h45m".
//    Valid time units are "ns", "us" (or "µs"), "ms", "s", "m", "h".
func registerSink(protocol string) error {
	return zap.RegisterSink(protocol, func(u *url.URL) (zap.Sink, error) {
		topicName := path.Join(u.Host, u.Path)

		publishTimeout := defaultPublishTimeout
		publishTimeoutRaw := u.Query().Get("publishTimeout")
		if publishTimeoutRaw != "" {
			var err error
			publishTimeout, err = time.ParseDuration(publishTimeoutRaw)
			if err != nil {
				return nil, err
			}
		}
		ctx := context.Background()
		topic, err := pubsub.OpenTopic(ctx, fmt.Sprintf("%s://%s", protocol, topicName))
		if err != nil {
			return nil, err
		}

		return &pubsubSink{topic: topic, publishTimeout: publishTimeout}, nil
	})
}

// pubsubSink is struct that satisfies zap.Sink
type pubsubSink struct {
	topic *pubsub.Topic

	publishTimeout time.Duration
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
		ctx, cancel := context.WithTimeout(context.Background(), zpb.publishTimeout)
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
