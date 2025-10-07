package operation

import (
	"context"
	"db-worker/internal/config/operation"
	builder_pkg "db-worker/internal/service/builder"
	"db-worker/internal/storage"
	"errors"
	"fmt"
	"sync"
	"time"

	"github.com/sirupsen/logrus"
)

func (s *Service) readMessages(ctx context.Context) {
	logrus.WithFields(logrus.Fields{
		"name":       s.cfg.Name,
		"connection": s.cfg.Request.From,
	}).Info("operation: start read messages")

	for {
		select {
		case <-ctx.Done():
			logrus.WithFields(logrus.Fields{
				"name":       s.cfg.Name,
				"connection": s.cfg.Request.From,
			}).Debug("operation: context done")

			return
		case <-s.quitChan:
			logrus.WithFields(logrus.Fields{
				"name":       s.cfg.Name,
				"connection": s.cfg.Request.From,
			}).Debug("operation: quit channel received")

			return
		case msg, ok := <-s.msgChan:
			if !ok {
				logrus.WithFields(logrus.Fields{
					"name":       s.cfg.Name,
					"connection": s.cfg.Request.From,
				}).Debug("operation: message channel closed")

				return
			}

			if err := s.processMessage(ctx, msg); err != nil {
				logrus.WithError(err).Error("operation: error process message")
				continue
			}

			logrus.WithFields(logrus.Fields{
				"name":       s.cfg.Name,
				"message":    msg,
				"connection": s.cfg.Request.From,
			}).Debug("operation: message processed")
		}
	}
}

// processMessage обрабатывает сообщение - валидирует, строит запросы и выполняет их.
func (s *Service) processMessage(ctx context.Context, msg map[string]interface{}) error {
	logrus.WithFields(logrus.Fields{
		"name":       s.cfg.Name,
		"message":    msg,
		"connection": s.cfg.Request.From,
	}).Debug("operation: received message")

	err := s.validateMessage(msg)
	if err != nil {
		return fmt.Errorf("error validate message: %w", err)
	}

	logrus.WithFields(logrus.Fields{
		"name":       s.cfg.Name,
		"message":    msg,
		"connection": s.cfg.Request.From,
	}).Debug("operation: message validated")

	requests := s.buildRequest(msg)

	var wg sync.WaitGroup
	wg.Add(len(requests))

	var errs []error

	for _, r := range requests {
		go func(req request, wg *sync.WaitGroup) {
			defer wg.Done()

			if err := s.execRequest(ctx, &req); err != nil {
				errs = append(errs, err)
			}
		}(r, &wg)
	}

	wg.Wait()

	if len(errs) > 0 {
		return fmt.Errorf("error exec requests: %w", errors.Join(errs...))
	}

	return nil
}

func (s *Service) execRequest(ctx context.Context, req *request) error {
	timeoutCtx, cancel := context.WithTimeout(ctx, time.Duration(s.cfg.Timeout)*time.Millisecond)
	defer cancel()

	err := req.storage.driver.Exec(timeoutCtx, req.request)
	if err != nil {
		return fmt.Errorf("error exec request: %w", err)
	}

	return nil
}

type request struct {
	storage drivers
	request *storage.Request
}

func (s *Service) buildRequest(msg map[string]interface{}) []request {
	res := make([]request, 0, len(s.storagesMap))

	for _, storage := range s.driversMap {
		var builder builder_pkg.Builder

		switch storage.driver.Type() {
		case operation.StorageTypePostgres:
			builder = builder_pkg.ForPostgres()
		default:
			logrus.WithFields(logrus.Fields{
				"name":    s.cfg.Name,
				"storage": storage.driver.Name(),
				"type":    storage.driver.Type(),
			}).Error("operation: unknown storage type")

			continue
		}

		builder = builder.WithOperation(*s.cfg).WithValues(msg).WithTable(storage.cfg.Table)

		switch s.cfg.Type {
		case operation.OperationTypeCreate:
			builder = builder.WithCreateOperation()
		case operation.OperationTypeUpdate:
			// not implemented
			continue
		case operation.OperationTypeDelete:
			// not implemented
			continue
		case operation.OperationTypeDeleteAll:
			// not implemented
			continue
		}

		req, err := builder.Build()
		if err != nil {
			logrus.WithError(err).Errorf("operation %s: error build request", s.cfg.Name)
			continue
		}

		res = append(res, request{
			storage: storage,
			request: req,
		})
	}

	return res
}
