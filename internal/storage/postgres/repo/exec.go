package postgres

import (
	"context"
	"db-worker/internal/storage"
	"fmt"
	"time"
)

// Begin начинает транзакцию.
func (db *Repo) Begin(ctx context.Context, id string) error {
	if _, err := db.getTx(id); err == nil {
		return nil
	}

	db.transaction.mu.Lock()
	defer db.transaction.mu.Unlock()

	tx, err := db.db.BeginTx(ctx, nil)
	if err != nil {
		return fmt.Errorf("error on begin transaction: %w", err)
	}

	db.transaction.tx[id] = tx

	return nil
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

	ctx, cancel := context.WithTimeout(ctx, time.Duration(db.insertTimeout)*time.Millisecond)

	go func() {
		ticker := time.NewTicker(time.Duration(db.insertTimeout) * time.Millisecond)
		defer ticker.Stop()

		for {
			select {
			case <-ticker.C:
				cancel()
			case <-ctx.Done():
				cancel()
			}
		}
	}()

	_, err = tx.ExecContext(ctx, sql, args...)
	if err != nil {
		return fmt.Errorf("error executing query: %w", err)
	}

	return nil
}

// Commit коммитит транзакцию.
//
//nolint:dupl // одинаковая логика для таймаутов.
func (db *Repo) Commit(ctx context.Context, id string) error {
	tx, err := db.getTx(id)
	if err != nil {
		return fmt.Errorf("error getting transaction: %w", err)
	}

	ctx, cancel := context.WithTimeout(ctx, time.Duration(db.insertTimeout)*time.Millisecond)

	go func() {
		ticker := time.NewTicker(time.Duration(db.insertTimeout) * time.Millisecond)
		defer ticker.Stop()

		for {
			select {
			case <-ticker.C:
				cancel()
			case <-ctx.Done():
				cancel()
			}
		}
	}()

	err = tx.Commit()
	if err != nil {
		return fmt.Errorf("error committing transaction: %w", err)
	}

	db.transaction.mu.Lock()
	defer db.transaction.mu.Unlock()

	delete(db.transaction.tx, id)

	return nil
}

// Rollback откатывает транзакцию.
//
//nolint:dupl // одинаковая логика для таймаутов.
func (db *Repo) Rollback(ctx context.Context, id string) error {
	tx, err := db.getTx(id)
	if err != nil {
		return fmt.Errorf("error getting transaction: %w", err)
	}

	ctx, cancel := context.WithTimeout(ctx, time.Duration(db.insertTimeout)*time.Millisecond)

	go func() {
		ticker := time.NewTicker(time.Duration(db.insertTimeout) * time.Millisecond)
		defer ticker.Stop()

		for {
			select {
			case <-ticker.C:
				cancel()
			case <-ctx.Done():
				cancel()
			}
		}
	}()

	err = tx.Rollback()
	if err != nil {
		return fmt.Errorf("error rolling back transaction: %w", err)
	}

	db.transaction.mu.Lock()
	defer db.transaction.mu.Unlock()

	delete(db.transaction.tx, id)

	return nil
}

// FinishTx завершает транзакцию без коммита (cleanup при неуспехе begin в другом драйвере).
func (db *Repo) FinishTx(ctx context.Context, id string) error {
	db.transaction.mu.Lock()
	defer db.transaction.mu.Unlock()

	tx, ok := db.transaction.tx[id]
	if !ok {
		return nil // ничего не делаем — уже очищено (либо commit, либо rollback)
	}

	ctx, cancel := context.WithTimeout(ctx, time.Duration(db.insertTimeout)*time.Millisecond)

	go func() {
		ticker := time.NewTicker(time.Duration(db.insertTimeout) * time.Millisecond)
		defer ticker.Stop()

		for {
			select {
			case <-ticker.C:
				cancel()
			case <-ctx.Done():
				cancel()
			}
		}
	}()

	// Игнорируем ошибку Rollback — цель: гарантированно освободить ресурсы
	_ = tx.Rollback()
	delete(db.transaction.tx, id)

	return nil
}
