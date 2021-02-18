package pubzap_test

import (
	"context"
	"encoding/json"
	"fmt"
	"strings"
	"sync"
	"testing"

	"cloud.google.com/go/pubsub"
	"cloud.google.com/go/pubsub/pstest"
	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
	"google.golang.org/api/option"
	"google.golang.org/grpc"
)

type exampleObj struct {
	A int
	B string
	C []byte
}

func (e exampleObj) MarshalLogObject(enc zapcore.ObjectEncoder) error {
	enc.AddInt("A", e.A)
	enc.AddString("B", e.B)
	enc.AddByteString("C", e.C)

	return nil
}

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
		MessageKey:     "msg",
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
		go func(v int) {
			logger.Info(
				fmt.Sprintf("message %d", v),
				zap.Int("key1", v),
				zap.Int("key2", v),
				zap.Int("key3", v),
				zap.String("stringKey", strings.Repeat("value", 90)),
				zap.Object("exampleObj", exampleObj{B: strings.Repeat("B", 100), A: 9999999, C: []byte(strings.Repeat("C", 100))}),
			)
		}(i)
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
		var v interface{}
		errJSON := json.Unmarshal(m.Data, &v)
		if errJSON != nil || v == nil {
			cancel()
			m.Ack()
			return
		}

		mu.Lock()
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
