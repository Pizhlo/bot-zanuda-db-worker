package uow

import (
	"bytes"
	"context"
	"db-worker/internal/storage"
	"fmt"

	"github.com/sirupsen/logrus"
)

// LoadOnStartup загружает запросы из базы данных при запуске сервиса и выполняет их.
func (s *Service) LoadOnStartup(ctx context.Context) (err error) {
	logrus.WithFields(logrus.Fields{
		"name":        s.cfg.Name,
		"instance_id": s.instanceID,
	}).Info("loading requests from database on startup")

	// сначала выгружаем уже завершенные транзакции и обновляем метрики
	if err := s.setupMetrics(ctx); err != nil {
		return fmt.Errorf("error setup metrics: %w", err)
	}

	// теперь выгружаем транзакции в статусе in progress и обрабатываем их
	txModels, err := s.requestsRepo.GetAllTransactionsByFields(ctx, map[string]any{
		"status":         string(storage.TxStatusInProgress),
		"instance_id":    s.instanceID,
		"operation_type": s.cfg.Type,
	})
	if err != nil {
		return fmt.Errorf("error get all requests by status, operation type and instance id: %w", err)
	}

	logrus.WithFields(logrus.Fields{
		"name":               s.cfg.Name,
		"instance_id":        s.instanceID,
		"transactions_count": len(txModels),
	}).Info("transactions loaded from database")

	// список айди для обновления статуса на CANCELED, т.к. изменилась конфигурация операции
	updateIDs := make([]string, 0, len(txModels))

	for _, txModel := range txModels {
		// Для отмененных транзакций добавляем метрики здесь
		s.addTotalTransactions(1)
		s.addInProgressTransactions(1)

		if !compareOperationHash(txModel.OperationHash, s.cfg.Hash) {
			updateIDs = append(updateIDs, txModel.ID)
			continue
		}

		err = s.processTxModel(ctx, txModel)
		if err != nil {
			return fmt.Errorf("error process tx model: %w", err)
		}
	}

	if len(updateIDs) > 0 {
		err = s.processCanceledTransactions(ctx, updateIDs)
		if err != nil {
			return fmt.Errorf("error process canceled transactions: %w", err)
		}
	}

	logrus.WithFields(logrus.Fields{
		"name":           s.cfg.Name,
		"instance_id":    s.instanceID,
		"requests_count": len(txModels),
		"update_ids":     updateIDs,
	}).Info("requests executed on startup successfully")

	// processTxModel сам обрабатывает метрики при ошибке, поэтому не нужно вызывать addFailedTransactions здесь
	// для транзакций, которые обрабатываются через processTxModel
	// execTx сам обрабатывает метрики при успехе

	return nil
}

func (s *Service) processCanceledTransactions(ctx context.Context, updateIDs []string) error {
	logrus.WithFields(logrus.Fields{
		"name":           s.cfg.Name,
		"instance_id":    s.instanceID,
		"requests_count": len(updateIDs),
		"update_ids":     updateIDs,
	}).Info("updating status of requests to CANCELED")

	err := s.requestsRepo.UpdateStatusMany(ctx, updateIDs, string(storage.TxStatusCanceled), "operation configuration changed")
	if err != nil {
		s.addFailedTransactions(len(updateIDs))
		return fmt.Errorf("error update status many requests: %w", err)
	}

	s.addCanceledTransactions(len(updateIDs))

	return nil
}

// setupMetrics загружает количество завершенных транзакций из базы данных.
// Нужно, чтобы восстановить метрики: сколько успешных транзакций, в процессе, отмененных и т.п.
func (s *Service) setupMetrics(ctx context.Context) error {
	if err := s.setupSuccessTransactionMetrics(ctx); err != nil {
		return fmt.Errorf("error setup success transaction metrics: %w", err)
	}

	if err := s.setupFailedTransactionMetrics(ctx); err != nil {
		return fmt.Errorf("error setup failed transaction metrics: %w", err)
	}

	if err := s.setupCanceledTransactionMetrics(ctx); err != nil {
		return fmt.Errorf("error setup canceled transaction metrics: %w", err)
	}

	// не добавляем in progress, т.к. они добавятся в рантайме

	return nil
}

//nolint:dupl // похожая реализация, с разницей в устанавливаемых метриках
func (s *Service) setupSuccessTransactionMetrics(ctx context.Context) error {
	logrus.WithFields(logrus.Fields{
		"name":        s.cfg.Name,
		"instance_id": s.instanceID,
	}).Info("loading success transactions count from database")

	count, err := s.requestsRepo.GetCountTransactionsByFields(ctx, map[string]any{
		"status":         string(storage.TxStatusSuccess),
		"instance_id":    s.instanceID,
		"operation_type": s.cfg.Type,
	})
	if err != nil {
		return fmt.Errorf("error get count transactions by fields: %w", err)
	}

	s.metricsService.AddSuccessTransactions(count)
	s.metricsService.AddTotalTransactions(count)

	return nil
}

//nolint:dupl // похожая реализация, с разницей в устанавливаемых метриках
func (s *Service) setupFailedTransactionMetrics(ctx context.Context) error {
	logrus.WithFields(logrus.Fields{
		"name":        s.cfg.Name,
		"instance_id": s.instanceID,
	}).Info("loading failed transactions count from database")

	count, err := s.requestsRepo.GetCountTransactionsByFields(ctx, map[string]any{
		"status":         string(storage.TxStatusFailed),
		"instance_id":    s.instanceID,
		"operation_type": s.cfg.Type,
	})
	if err != nil {
		return fmt.Errorf("error get count transactions by fields: %w", err)
	}

	s.metricsService.AddFailedTransactions(count)
	s.metricsService.AddTotalTransactions(count)

	return nil
}

//nolint:dupl // похожая реализация, с разницей в устанавливаемых метриках
func (s *Service) setupCanceledTransactionMetrics(ctx context.Context) error {
	logrus.WithFields(logrus.Fields{
		"name":        s.cfg.Name,
		"instance_id": s.instanceID,
	}).Info("loading canceled transactions count from database")

	count, err := s.requestsRepo.GetCountTransactionsByFields(ctx, map[string]any{
		"status":         string(storage.TxStatusCanceled),
		"instance_id":    s.instanceID,
		"operation_type": s.cfg.Type,
	})
	if err != nil {
		return fmt.Errorf("error get count transactions by fields: %w", err)
	}

	s.metricsService.AddCanceledTransactions(count)
	s.metricsService.AddTotalTransactions(count)

	return nil
}

// processTxModel обрабатывает модель транзакции.
// Строит запросы, сохраняет их и выполняет транзакцию.
func (s *Service) processTxModel(ctx context.Context, txModel storage.TransactionModel) (err error) {
	defer func() {
		if r := recover(); r != nil {
			s.addFailedTransactions(1)
			panic(r)
		}

		if err != nil {
			s.addFailedTransactions(1)
		} else {
			s.addSuccessTransactions(1)
		}
	}()

	var reqs map[storage.Driver]*storage.Request

	reqs, err = s.BuildRequests(txModel.Data, s.userDriversMap, *s.cfg)
	if err != nil {
		err = fmt.Errorf("error build requests: %w", err)
		return
	}

	tx := storage.NewTransactionFromModel(&txModel)
	tx.SaveRequests(reqs)

	if err = s.execTx(ctx, tx); err != nil {
		err = fmt.Errorf("error exec tx: %w", err)
		return
	}

	return nil
}

func compareOperationHash(operationHash []byte, cfgHash []byte) bool {
	return bytes.Equal(operationHash, cfgHash)
}
