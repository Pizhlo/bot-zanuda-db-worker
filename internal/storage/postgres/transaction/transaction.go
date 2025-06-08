package transaction

import (
	"context"
	"db-worker/internal/model"
	interfaces "db-worker/internal/service/message/interface"
	"encoding/json"
	"fmt"
	"time"

	"github.com/sirupsen/logrus"
)

func (s *Repo) CreateTx(ctx context.Context, id string, notes []interfaces.Message) error {
	ctx, cancel := context.WithTimeout(ctx, time.Duration(s.insertTimeout)*time.Millisecond)
	defer cancel()

	tx, err := s.getOrCreateTx(ctx, id)
	if err != nil {
		return fmt.Errorf("error creating transaction: %w", err)
	}

	// Создаем транзакцию
	createTxQuery := `
		INSERT INTO transactions.transactions (id, status, created_at, instance_id)
		VALUES ($1, 'in progress', extract(epoch from current_timestamp)::BIGINT, $2)
	`

	_, err = tx.ExecContext(ctx, createTxQuery, id, s.instanceID)
	if err != nil {
		return fmt.Errorf("error saving transaction: %w", err)
	}

	// Сохраняем запросы
	saveRequestsQuery := `
		INSERT INTO transactions.requests (id, entity, data, operation, tx_id)
		VALUES ($1, $2, $3, $4, $5)
	`

	stmt, err := tx.PrepareContext(ctx, saveRequestsQuery)
	if err != nil {
		return fmt.Errorf("error preparing statement: %w", err)
	}
	defer stmt.Close()

	for _, note := range notes {
		model := note.Model().(model.CreateNoteRequest)

		data, err := json.Marshal(model)
		if err != nil {
			return fmt.Errorf("error marshalling model: %w", err)
		}

		_, err = stmt.ExecContext(ctx, model.RequestID, "notes", data, model.Operation, id)
		if err != nil {
			return fmt.Errorf("error saving request: %w", err)
		}
	}

	logrus.Debugf("Transaction Saver: transaction created and requests saved. transaction id: %s", id)

	return s.Commit(ctx, id)
}

func (s *Repo) SaveResult(ctx context.Context, id string, result model.Result) error {
	ctx, cancel := context.WithTimeout(ctx, time.Duration(s.insertTimeout)*time.Millisecond)
	defer cancel()

	tx, err := s.getOrCreateTx(ctx, id)
	if err != nil {
		return fmt.Errorf("error creating transaction: %w", err)
	}

	var query string
	args := []any{}

	if result.Status == "failed" {
		query = `
		UPDATE transactions.transactions 
		SET status = $2::tx_status, error = $3
		WHERE id = $1
	`
		args = append(args, id, result.Status, result.Error)
	} else {
		query = `
		UPDATE transactions.transactions 
		SET status = $2::tx_status
		WHERE id = $1
	`
		args = append(args, id, result.Status)
	}

	_, err = tx.ExecContext(ctx, query, args...)
	if err != nil {
		return fmt.Errorf("error updating transaction result: %w", err)
	}

	logrus.Debugf("transaction result saved. transaction id: %s", id)

	return s.Commit(ctx, id)
}
