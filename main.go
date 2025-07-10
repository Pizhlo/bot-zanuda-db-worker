package main

import (
	"context"
	"db-worker/internal/app"
	"flag"
	"os"
	"os/signal"
	"sync"
	"syscall"

	"github.com/sirupsen/logrus"
)

func main() {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	configPath := flag.String("config", "internal/config/config.yaml", "путь к файлу конфигурации")
	flag.Parse()

	app, err := app.NewApp(ctx, *configPath)
	if err != nil {
		logrus.Fatalf("error creating app: %+v", err)
	}

	notifyCtx, notify := signal.NotifyContext(ctx, syscall.SIGINT, syscall.SIGTERM, os.Interrupt)
	defer notify()

	<-notifyCtx.Done()
	logrus.Info("shutdown")

	var wg sync.WaitGroup

	wg.Add(1)
	go func(wg *sync.WaitGroup) {
		defer wg.Done()

		app.NoteCreator.Close()
		app.TxSaver.Close()

		err = app.Rabbit.Close()
		if err != nil {
			logrus.Errorf("error closing rabbit: %+v", err)
		}
	}(&wg)

	wg.Wait()

	notify()
}
