package pubzap

import (
	"context"
	"fmt"
	"io"
	"net/url"
	"strings"

	"cloud.google.com/go/pubsub"
	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
)

/*
	url format:
 	pubsub://projects/YOUR-PROJECT-ID/topics/YOUR-TOPIC
*/
func parseURL(u *url.URL) (string, string, error) {
	splitted := strings.Split(u.Path, "/")
	if len(splitted) != 4 {
		return "", "", fmt.Errorf("expect path be in format YOUR-PROJECT-ID/topics/YOUR-TOPIC: got %v", u.Path)
	}

	return splitted[1], splitted[3], nil
}

// we need to tell zap to recognize pubsub urls.
func init() {
	if err := zap.RegisterSink("pubsub", func(u *url.URL) (zap.Sink, error) {
		projectID, topic, err := parseURL(u)
		if err != nil {
			return nil, err
		}

		pbClient, err := pubsub.NewClient(context.Background(), projectID)
		if err != nil {
			return nil, err
		}
		pbTopic := pbClient.Topic(topic)

		return &pubsubSink{pbClient: pbClient, pbTopic: pbTopic}, nil
	}); err != nil {
		panic(err)
	}
}

// pubsubSink is struct that satisfies zap.Sink
type pubsubSink struct {
	pbClient *pubsub.Client
	pbTopic  *pubsub.Topic

	zapcore.WriteSyncer
	io.Closer
}

// Close implement io.Closer.
func (zpb *pubsubSink) Close() error {
	return zpb.pbClient.Close()
}

// Write implement zap.Sink func Write
// Non-block publish to Pubsub by omitting result check.
func (zpb *pubsubSink) Write(b []byte) (n int, err error) {
	_ = zpb.pbTopic.Publish(context.Background(), &pubsub.Message{
		Data: b,
	})

	return 0, err
}

// Sync implement zap.Sink func Sync. In fact, we do nothing here.
func (zpb *pubsubSink) Sync() error {
	return nil
}
