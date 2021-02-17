package main

import (
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"log"
	"sync"

	"cloud.google.com/go/pubsub"
	"cloud.google.com/go/pubsub/pstest"
	_ "github.com/Funfun/pubzap"
	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
	"google.golang.org/api/option"
	"google.golang.org/grpc"
)

var (
	topic     = flag.String("topic", "", "GCP topic")
	projectID = flag.String("projectID", "", "GCP projectID")
)

func main() {
	flag.Parse()

	if *topic == "" {
		log.Fatal("Topic can not be empty")
	}

	if *projectID == "" {
		log.Fatal("projectID can not be empty")
	}

	ctx := context.Background()
	// Start a fake server running locally.
	srv := pstest.NewServer()
	defer srv.Close()

	topicInfo, close, err := zap.Open(fmt.Sprintf("pubsub://projects/%s/topics/%s?srvAddr=%s", *projectID, *topic, srv.Addr))
	defer close()
	if err != nil {
		log.Fatal(err)
	}
	encoderCfg := zapcore.EncoderConfig{
		MessageKey:     "message",
		LevelKey:       "severity",
		NameKey:        "logger",
		LineEnding:     " ", // intention space
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
		go func(idx int) {
			logger.Info(fmt.Sprintf("message %d", idx), zap.Int("key", idx))
		}(i)
	}

	// Connect to the server without using TLS.
	conn, err := grpc.Dial(srv.Addr, grpc.WithInsecure())
	if err != nil {
		log.Fatal(err)
	}
	defer conn.Close()

	// Use the connection when creating a pubsub client.
	client, err := pubsub.NewClient(ctx, *projectID, option.WithGRPCConn(conn))
	if err != nil {
		log.Fatal(err)
	}
	defer client.Close()

	topic, err := client.CreateTopic(ctx, *topic)
	if err != nil {
		log.Fatal(err)
	}

	sub, err := client.CreateSubscription(ctx, "sub-name", pubsub.SubscriptionConfig{Topic: topic})
	if err != nil {
		log.Fatal(err)
	}

	ctx, cancel := context.WithCancel(ctx)

	var mu sync.Mutex
	count := 0

	err = sub.Receive(ctx, func(ctx context.Context, m *pubsub.Message) {
		mu.Lock()
		v := map[string]interface{}{}
		fmt.Printf("%s\n", m.Data)
		if errJSON := json.Unmarshal(m.Data, &v); errJSON != nil {
			log.Printf("unmarshal err: %s\n", errJSON)
			cancel()
		}
		log.Printf("received message: %v\n", v)

		count++
		if count >= 10 {
			cancel()
		}
		mu.Unlock()
		m.Ack()
	})
	if err != nil {
		log.Fatal(err)
	}
}
