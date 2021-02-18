package pubzap_test

import (
	"context"
	"encoding/json"
	"fmt"
	"sync"
	"testing"

	"cloud.google.com/go/pubsub"
	"cloud.google.com/go/pubsub/pstest"
	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
	"google.golang.org/api/option"
	"google.golang.org/grpc"
)

func TestPublishAndRecieveLogs(t *testing.T) {
	projectID := "project"
	topicName := "topic"
	subName := "subscription"

	ctx := context.Background()
	// Start a fake server running locally.
	srv := pstest.NewServer()
	defer srv.Close()

	topicInfo, close, err := zap.Open(fmt.Sprintf("pubsub://projects/%s/topics/%s?srvAddr=%s", projectID, topicName, srv.Addr))
	defer close()
	if err != nil {
		t.Fatal(err)
	}
	encoderCfg := zapcore.EncoderConfig{
		MessageKey:     "message",
		LevelKey:       "severity",
		NameKey:        "logger",
		LineEnding:     zapcore.DefaultLineEnding,
		EncodeLevel:    zapcore.LowercaseLevelEncoder,
		EncodeTime:     zapcore.RFC3339NanoTimeEncoder,
		EncodeDuration: zapcore.StringDurationEncoder,
	}
	pbencoder := zapcore.NewJSONEncoder(encoderCfg)

	core := zapcore.NewTee(
		zapcore.NewCore(pbencoder, topicInfo, zapcore.InfoLevel),
	)
	logger := zap.New(core)
	defer logger.Sync()

	for i := 1; i <= 10; i++ {
		logger.Info(fmt.Sprintf("message %d", i), zap.Int("key", i))
	}

	// Connect to the server without using TLS.
	conn, err := grpc.Dial(srv.Addr, grpc.WithInsecure())
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()

	// Use the connection when creating a pubsub client.
	client, err := pubsub.NewClient(ctx, projectID, option.WithGRPCConn(conn))
	if err != nil {
		t.Fatal(err)
	}
	defer client.Close()

	topic, err := client.CreateTopic(ctx, topicName)
	if err != nil {
		t.Fatal(err)
	}
	defer topic.Stop()

	sub, err := client.CreateSubscription(ctx, subName, pubsub.SubscriptionConfig{Topic: topic})
	if err != nil {
		t.Fatal(err)
	}

	ctx, cancel := context.WithCancel(ctx)

	var mu sync.Mutex
	count := 0
	wantCount := 10

	err = sub.Receive(ctx, func(ctx context.Context, m *pubsub.Message) {
		mu.Lock()
		v := map[string]interface{}{}
		if errJSON := json.Unmarshal(m.Data, &v); errJSON != nil {
			cancel()
		}

		count++
		if count >= wantCount {
			cancel()
		}
		mu.Unlock()
		m.Ack()
	})
	if err != nil {
		t.Fatalf("expect no error, got: %s", err)
	}

	if count != 10 {
		t.Fatalf("expect count to be %d, got: %d", wantCount, count)
	}
}
