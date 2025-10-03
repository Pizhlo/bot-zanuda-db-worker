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

//nolint:gocognit // это точка входа в программу. Будет совершен рефактор (задача BZ-13).
func main() {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	configPath := flag.String("config", "./config.yaml", "path to config file")
	operationsConfigPath := flag.String("operations-config", "./operations.yaml", "path to operations config file")
	flag.Parse()

	app, err := app.NewApp(ctx, *configPath, *operationsConfigPath)
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

		app.TxSaver.Close()

		for name, worker := range app.Workers {
			err = worker.Close()
			if err != nil {
				logrus.Errorf("error closing worker %s: %+v", name, err)
			}
		}

		for name, storage := range app.Storages {
			err = storage.Close()
			if err != nil {
				logrus.Errorf("error closing storage %s: %+v", name, err)
			}
		}

		for name, operation := range app.Operations {
			err = operation.Close()
			if err != nil {
				logrus.Errorf("error closing operation %s: %+v", name, err)
			}
		}
	}(&wg)

	wg.Wait()

	notify()
}
