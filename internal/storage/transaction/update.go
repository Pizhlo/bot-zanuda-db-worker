package transaction

import (
	"context"
	"database/sql"
	"errors"
	"fmt"

	"github.com/huandu/go-sqlbuilder"
)

// UpdateStatusMany обновляет статус транзакций по айдишникам.
func (r *Repo) UpdateStatusMany(ctx context.Context, ids []string, status string, errMsg string) error {
	if len(ids) == 0 {
		return nil
	}

	tx, err := r.db.BeginTx(ctx, nil)
	if err != nil {
		return fmt.Errorf("error beginning transaction: %w", err)
	}

	defer func() {
		if err != nil {
			if rollbackErr := tx.Rollback(); rollbackErr != nil && !errors.Is(rollbackErr, sql.ErrTxDone) {
				err = fmt.Errorf("%w; rollback failed: %v", err, rollbackErr)
			}
		}
	}()

	ub := sqlbuilder.NewUpdateBuilder()
	ub.SetFlavor(sqlbuilder.PostgreSQL)
	ub.Update("transactions.transactions")
	ub.Set(
		ub.Assign("status", status),
		ub.Assign("error", errMsg),
	)

	// Преобразуем []string в []interface{} для метода In
	idValues := make([]interface{}, len(ids))
	for i, id := range ids {
		idValues[i] = id
	}

	ub.Where(ub.In("id", idValues...))

	query, args := ub.Build()

	_, err = tx.ExecContext(ctx, query, args...)
	if err != nil {
		return fmt.Errorf("error updating status for transactions by ids: %w", err)
	}

	return tx.Commit()
}
