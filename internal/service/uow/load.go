package uow

import (
	"bytes"
	"context"
	"db-worker/internal/storage"
	"fmt"

	"github.com/sirupsen/logrus"
)

// LoadOnStartup загружает запросы из базы данных при запуске сервиса и выполняет их.
func (s *Service) LoadOnStartup(ctx context.Context) error {
	logrus.WithFields(logrus.Fields{
		"name":        s.cfg.Name,
		"instance_id": s.instanceID,
	}).Info("loading requests from database on startup")

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
		if !compareOperationHash(txModel.OperationHash, s.cfg.Hash) {
			updateIDs = append(updateIDs, txModel.ID)
			continue
		}

		err := s.processTxModel(ctx, txModel)
		if err != nil {
			return fmt.Errorf("error process tx model: %w", err)
		}
	}

	if len(updateIDs) > 0 {
		logrus.WithFields(logrus.Fields{
			"name":           s.cfg.Name,
			"instance_id":    s.instanceID,
			"requests_count": len(updateIDs),
			"update_ids":     updateIDs,
		}).Info("updating status of requests to CANCELED")

		err := s.requestsRepo.UpdateStatusMany(ctx, updateIDs, string(storage.TxStatusCanceled), "operation configuration changed")
		if err != nil {
			return fmt.Errorf("error update status many requests: %w", err)
		}
	}

	logrus.WithFields(logrus.Fields{
		"name":           s.cfg.Name,
		"instance_id":    s.instanceID,
		"requests_count": len(txModels),
		"update_ids":     updateIDs,
	}).Info("requests executed on startup successfully")

	return nil
}

// processTxModel обрабатывает модель транзакции.
// Строит запросы, сохраняет их и выполняет транзакцию.
func (s *Service) processTxModel(ctx context.Context, txModel storage.TransactionModel) error {
	reqs, err := s.BuildRequests(txModel.Data, s.userDriversMap, *s.cfg)
	if err != nil {
		return fmt.Errorf("error build requests: %w", err)
	}

	tx := storage.NewTransactionFromModel(&txModel)
	tx.SaveRequests(reqs)

	return s.execTx(ctx, tx)
}

func compareOperationHash(operationHash []byte, cfgHash []byte) bool {
	return bytes.Equal(operationHash, cfgHash)
}
