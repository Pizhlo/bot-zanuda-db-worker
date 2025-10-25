package transaction

import (
	"context"
	"db-worker/internal/service/builder"
	"db-worker/internal/storage"
	"errors"
	"fmt"
)

func (s *Repo) SaveTx(ctx context.Context, tx storage.TransactionEditor) error {
	dbTx, err := s.getOrCreateTx(ctx, tx.ID())
	if err != nil {
		return fmt.Errorf("error creating transaction: %w", err)
	}

	// Создаем транзакцию
	createTxQuery := `
		INSERT INTO transactions.transactions (id, status, error, instance_id, failed_driver, operation_hash)
		VALUES ($1, $2, $3, $4, $5, $6)
	`

	_, err = dbTx.ExecContext(ctx, createTxQuery, tx.ID, tx.Status, tx.Error(), tx.InstanceID(), tx.FailedDriver(), tx.OperationHash())
	if err != nil {
		return fmt.Errorf("error saving transaction: %w", err)
	}

	// поля, которые будут переданы билдеру, для сохранения запросов в базе
	fields := map[string]any{
		"tx_id": tx.ID,
	}

	for driver, req := range tx.Requests() {
		fields["driver_name"] = driver.Name()
		fields["driver_type"] = driver.Type()
		fields["data"] = req.Raw
	}

	b := builder.ForPostgres().WithTable("transactions.requests").WithCreateOperation().WithValues(fields)

	req, err := b.Build()
	if err != nil {
		return fmt.Errorf("error building request for saving transaction's requests: %w", err)
	}

	_, err = dbTx.ExecContext(ctx, req.Val.(string), req.Args)
	if err != nil {
		return fmt.Errorf("error saving transaction's requests: %w", err)
	}

	// не коммитим, т.к. это будет сделано извне
	return nil
}

// UpdateTx обновляет транзакцию в кэше и хранилище.
func (s *Repo) UpdateTx(ctx context.Context, tx storage.TransactionEditor) error {
	return errors.New("not implemented")
}

// DeleteTx удаляет транзакцию из кэша и хранилища.
func (s *Repo) DeleteTx(ctx context.Context, tx storage.TransactionEditor) error {
	return errors.New("not implemented")
}

// Exec выполняет запрос.
func (db *Repo) Exec(ctx context.Context, req *storage.Request, id string) error {
	tx, err := db.getTx(id)
	if err != nil {
		return fmt.Errorf("error getting transaction: %w", err)
	}

	sql, ok := req.Val.(string)
	if !ok {
		return fmt.Errorf("request value is not a string")
	}

	args, ok := req.Args.([]any)
	if !ok {
		return fmt.Errorf("request arguments are not a slice of any")
	}

	_, err = tx.ExecContext(ctx, sql, args...)
	if err != nil {
		return fmt.Errorf("error executing query: %w", err)
	}

	return nil
}
