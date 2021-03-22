package main

import (
	"flag"
	"fmt"
	"log"
	"os"
	"time"

	_ "github.com/Funfun/pubzap"
	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
)

var (
	topicPath = flag.String("topicPath", "", "GCP topic path (including GCP projectID)")
)

func main() {
	flag.Parse()

	if *topicPath == "" {
		log.Fatal("Topic path can not be empty")
	}

	topicInfo, close, err := zap.Open(fmt.Sprintf("gcppubsub://%s", *topicPath))
	defer close()
	if err != nil {
		log.Fatal(err)
	}

	consoleDebugging := zapcore.Lock(os.Stdout)
	consoleEncoder := zapcore.NewConsoleEncoder(zap.NewDevelopmentEncoderConfig())

	pbencoder := zapcore.NewJSONEncoder(zap.NewProductionEncoderConfig())
	core := zapcore.NewTee(
		zapcore.NewCore(pbencoder, topicInfo, zapcore.InfoLevel),
		zapcore.NewCore(consoleEncoder, consoleDebugging, zapcore.DebugLevel),
	)

	logger := zap.New(core)
	defer logger.Sync()

	logger.Info("test msg for pubsub", zap.String("prodKey", "prodVal"))
	logger.Debug("debug msg (expect pubsub)", zap.Int("debugKey", 42))

	time.Sleep(10 * time.Second) // we need to wait since publish for pubsub is non-blocking
}
