package transaction

import (
	"context"
	"db-worker/internal/storage"
	"encoding/json"
	"fmt"

	"github.com/huandu/go-sqlbuilder"
	"github.com/sirupsen/logrus"
)

// GetAllTransactionsByFields получает все транзакции, удовлетворяющие условию.
//
//nolint:funlen // цельная логика получения, парсинга транзакций и запросов
func (r *Repo) GetAllTransactionsByFields(ctx context.Context, fields map[string]any) ([]storage.TransactionModel, error) {
	if len(fields) == 0 {
		return nil, fmt.Errorf("fields map is empty")
	}

	sb := sqlbuilder.NewSelectBuilder()
	sb.SetFlavor(sqlbuilder.PostgreSQL)
	sb.Select("transactions.transactions.id, status, data, error, instance_id, failed_driver, operation_hash, operation_type, created_at").
		From("transactions.transactions")

	// Строим where-условия динамически из всех полей
	whereConditions := buildWhereConditions(sb, fields)

	if len(whereConditions) == 0 {
		return nil, fmt.Errorf("no valid fields for where condition")
	}

	if len(whereConditions) == 1 {
		sb.Where(whereConditions[0])
	} else {
		sb.Where(sb.And(whereConditions...))
	}

	query, args := sb.Build()

	rows, err := r.db.QueryContext(ctx, query, args...)
	if err != nil {
		return nil, fmt.Errorf("error getting all transactions by fields: %w", err)
	}

	defer func() {
		if err := rows.Close(); err != nil {
			logrus.WithError(err).Error("GetAllTransactionsByFields: error closing rows")
		}
	}()

	transactions := make([]storage.TransactionModel, 0)

	for rows.Next() {
		var transaction storage.TransactionModel

		data := make([]byte, 0)

		err := rows.Scan(&transaction.ID, &transaction.Status, &data, &transaction.Error, &transaction.InstanceID, &transaction.FailedDriver, &transaction.OperationHash, &transaction.OperationType, &transaction.CreatedAt)
		if err != nil {
			return nil, fmt.Errorf("error scanning transaction: %w", err)
		}

		err = json.Unmarshal(data, &transaction.Data)
		if err != nil {
			return nil, fmt.Errorf("error unmarshalling transaction data: %w", err)
		}

		requests, err := r.getRequestsByTransactionID(ctx, transaction.ID)
		if err != nil {
			return nil, fmt.Errorf("error getting requests by transaction id: %w", err)
		}

		transaction.Requests = requests

		transactions = append(transactions, transaction)
	}

	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("error getting transactions by fields: %w", err)
	}

	return transactions, nil
}

func (r *Repo) getRequestsByTransactionID(ctx context.Context, transactionID string) ([]storage.RequestModel, error) {
	query := `
		SELECT id, tx_id, driver_type, driver_name
		FROM transactions.requests
		WHERE tx_id = $1
	`

	rows, err := r.db.QueryContext(ctx, query, transactionID)
	if err != nil {
		return nil, fmt.Errorf("error getting requests by transaction id: %w", err)
	}

	defer func() {
		if err := rows.Close(); err != nil {
			logrus.WithError(err).Error("getRequestsByTransactionID: error closing rows")
		}
	}()

	requests := make([]storage.RequestModel, 0)

	for rows.Next() {
		var request storage.RequestModel

		err := rows.Scan(&request.ID, &request.TxID, &request.DriverType, &request.DriverName)
		if err != nil {
			return nil, fmt.Errorf("error scanning request: %w", err)
		}

		requests = append(requests, request)
	}

	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("error getting requests by transaction id: %w", err)
	}

	return requests, nil
}

// buildWhereConditions строит where-условия динамически из всех полей с оператором равенства.
func buildWhereConditions(sb *sqlbuilder.SelectBuilder, fields map[string]any) []string {
	whereConditions := make([]string, 0, len(fields))

	for fieldName, fieldValue := range fields {
		if fieldValue == nil {
			continue
		}

		whereConditions = append(whereConditions, sb.Equal(fieldName, fieldValue))
	}

	return whereConditions
}

// GetCountTransactionsByFields получает количество транзакций, удовлетворяющих условию.
func (r *Repo) GetCountTransactionsByFields(ctx context.Context, fields map[string]any) (int, error) {
	if len(fields) == 0 {
		return 0, fmt.Errorf("fields map is empty")
	}

	sb := sqlbuilder.NewSelectBuilder()
	sb.SetFlavor(sqlbuilder.PostgreSQL)
	sb.Select("COUNT(*)").
		From("transactions.transactions")

	whereConditions := buildWhereConditions(sb, fields)
	if len(whereConditions) == 0 {
		return 0, fmt.Errorf("no valid fields for where condition")
	}

	if len(whereConditions) == 1 {
		sb.Where(whereConditions[0])
	} else {
		sb.Where(sb.And(whereConditions...))
	}

	query, args := sb.Build()

	row := r.db.QueryRowContext(ctx, query, args...)

	var count int

	err := row.Scan(&count)
	if err != nil {
		return 0, fmt.Errorf("error scanning count: %w", err)
	}

	if err := row.Err(); err != nil {
		return 0, fmt.Errorf("error getting count transactions by fields: %w", err)
	}

	return count, nil
}
