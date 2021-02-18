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
	"google.golang.org/api/option"
	"google.golang.org/grpc"
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

		var clientOptions option.ClientOption
		var conn *grpc.ClientConn
		// test ability
		if srvAddr := u.Query().Get("srvAddr"); srvAddr != "" {
			conn, err := grpc.Dial(srvAddr, grpc.WithInsecure())
			if err != nil {
				return nil, err
			}

			clientOptions = option.WithGRPCConn(conn)
		}

		var pbClient *pubsub.Client
		if clientOptions == nil {
			pbClient, err = pubsub.NewClient(context.Background(), projectID)
		} else {
			pbClient, err = pubsub.NewClient(context.Background(), projectID, clientOptions)
		}
		if err != nil {
			return nil, err
		}
		pbTopic := pbClient.Topic(topic)

		return &pubsubSink{pbClient: pbClient, pbTopic: pbTopic, pbConn: conn}, nil
	}); err != nil {
		panic(err)
	}
}

// pubsubSink is struct that satisfies zap.Sink
type pubsubSink struct {
	pbClient *pubsub.Client
	pbTopic  *pubsub.Topic
	pbConn   *grpc.ClientConn

	zapcore.WriteSyncer
	io.Closer
}

// Close implement io.Closer.
func (zpb *pubsubSink) Close() error {
	if zpb.pbConn != nil {
		if err := zpb.pbConn.Close(); err != nil {
			return err
		}
	}
	zpb.pbTopic.Stop()

	return zpb.pbClient.Close()
}

// Write implement zap.Sink func Write
// Non-block publish to Pubsub by omitting result check.
func (zpb *pubsubSink) Write(b []byte) (n int, err error) {
	_ = zpb.pbTopic.Publish(context.Background(), &pubsub.Message{
		Data: b,
	})

	return len(b), nil
}

// Sync implement zap.Sink func Sync. In fact, we do nothing here.
func (zpb *pubsubSink) Sync() error {
	return nil
}
