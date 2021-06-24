package pubzap_test

import (
	"context"
	"fmt"
	"log"
	"strings"
	"testing"

	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
	"gocloud.dev/pubsub"
	_ "gocloud.dev/pubsub/mempubsub"
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
	ctx := context.Background()
	topic, err := pubsub.OpenTopic(ctx, "mem://topicA")
	if err != nil {
		t.Fatal(err)
	}
	defer topic.Shutdown(ctx)

	sub, err := pubsub.OpenSubscription(ctx, "mem://topicA")
	if err != nil {
		t.Fatal(err)
	}
	defer sub.Shutdown(ctx)

	topicInfo, close, err := zap.Open("mem://topicA?publishTimeout=10s")
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
	wantCount := 10000

	for i := 1; i <= wantCount; i++ {
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

	ctx, cancel := context.WithCancel(ctx)
	defer cancel()

	count := 0
	// Loop on received messages.
	for {
		msg, err := sub.Receive(ctx)
		if err != nil {
			// Errors from Receive indicate that Receive will no longer succeed.
			log.Printf("Receiving message: %v", err)
			break
		}
		// Do work based on the message, for example:
		// fmt.Printf("Got message: %q\n", msg.Body)
		// Messages must always be acknowledged with Ack.
		msg.Ack()
		count++
		if count >= wantCount {
			break
		}
	}
	if count != wantCount {
		t.Fatalf("expect count to be %d, got: %d", wantCount, count)
	}
}
