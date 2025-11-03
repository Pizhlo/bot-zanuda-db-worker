package message

import (
	"context"
	"database/sql"
	"db-worker/internal/service/operation/message"
	"encoding/json"
	"errors"
	"fmt"
	"strings"

	"github.com/sirupsen/logrus"
)

// CreateMany создает множества сообщений.
//
//nolint:funlen // единый код для составления и выполнения запроса
func (r *Repo) CreateMany(ctx context.Context, messages []message.Message) (err error) {
	if len(messages) == 0 {
		return nil
	}

	// Начинаем транзакцию
	tx, err := r.db.BeginTx(ctx, nil)
	if err != nil {
		return fmt.Errorf("error beginning transaction: %w", err)
	}

	defer func() {
		if err != nil {
			logrus.WithFields(logrus.Fields{
				"service": "message",
				"error":   err,
			}).Error("rolling back transaction")

			if rollbackErr := tx.Rollback(); rollbackErr != nil && !errors.Is(rollbackErr, sql.ErrTxDone) {
				err = fmt.Errorf("%w; rollback failed: %v", err, rollbackErr)
			}
		}
	}()

	// Строим SQL запрос с множественными VALUES без форматирования через fmt.Sprintf
	var qb strings.Builder
	qb.WriteString("INSERT INTO ")
	qb.WriteString(r.table)
	qb.WriteString(" (id, data, status, error, driver_type, driver_name, instance_id, operation_hash) VALUES ")
	query := qb.String()

	// Подготавливаем аргументы для запроса
	args := make([]interface{}, 0, len(messages)*8) // 8 полей на сообщение
	placeholders := make([]string, 0, len(messages))
	argIndex := 1

	for _, msg := range messages {
		// Сериализуем Data в JSON
		jsonData, err := json.Marshal(msg.Data)
		if err != nil {
			return fmt.Errorf("error marshaling message data: %w", err)
		}

		// Добавляем аргументы для текущего сообщения
		args = append(args,
			msg.ID,
			jsonData, // JSONB автоматически обработается драйвером PostgreSQL
			string(msg.Status),
			msg.Error,
			msg.DriverType,
			msg.DriverName,
			msg.InstanceID,
			msg.OperationHash,
		)

		// Формируем placeholder для текущего сообщения
		placeholders = append(placeholders, fmt.Sprintf("($%d, $%d, $%d, $%d, $%d, $%d, $%d, $%d)",
			argIndex, argIndex+1, argIndex+2, argIndex+3, argIndex+4, argIndex+5, argIndex+6, argIndex+7))
		argIndex += 8
	}

	// Объединяем все placeholders
	query += strings.Join(placeholders, ", ")

	// Выполняем запрос
	_, err = tx.ExecContext(ctx, query, args...)
	if err != nil {
		err = fmt.Errorf("error executing insert query: %w", err)
		return
	}

	// Коммитим транзакцию
	if err = tx.Commit(); err != nil {
		err = fmt.Errorf("error committing transaction: %w", err)
		return
	}

	return nil
}
