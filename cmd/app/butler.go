package main

import (
	"context"
	"fmt"
	"reflect"
	"runtime"
	"strings"
	"sync"

	"github.com/sirupsen/logrus"
)

type Butler struct {
	BuildInfo *BuildInfo

	quit chan struct{}
	ctx  *context.Context

	// Для отслеживания количества запущенных горутин
	wg sync.WaitGroup
}

func NewButler(ctx *context.Context) *Butler {
	return &Butler{
		BuildInfo: ReadBuildInfo(),
		ctx:       ctx,
		quit:      make(chan struct{}),
	}
}

func (b *Butler) start(caller func() error) {
	b.wg.Add(1)

	go func() {
		defer b.wg.Done()

		fn := runtime.FuncForPC(reflect.ValueOf(caller).Pointer()).Name()
		fn = strings.TrimPrefix(fn, b.BuildInfo.Name+"/")

		if err := caller(); err != nil {
			logrus.WithError(err).Errorf("error in %s", fn)
			return
		}
	}()
}

type stopper interface {
	Stop(ctx context.Context) error
}

func (b *Butler) stop(svc stopper) {
	name := fmt.Sprintf("%T", svc)

	if err := svc.Stop(*b.ctx); err != nil {
		logrus.WithError(err).Errorf("dirty shutdown %s", name)
		return
	}

	logrus.Debugf("successfully stopped %s", name)
}

// waitForAll ждет завершения всех запущенных горутин.
func (b *Butler) waitForAll() {
	b.wg.Wait()
}
