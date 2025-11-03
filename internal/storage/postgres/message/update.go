package message

import (
	"context"
	"database/sql"
	"db-worker/internal/service/operation/message"
	"errors"
	"fmt"
	"strconv"
	"strings"

	"github.com/sirupsen/logrus"
)

// UpdateMany обновляет статус и ошибку для множества сообщений.
//
//nolint:funlen // единый код для составления и выполнения запроса
func (r *Repo) UpdateMany(ctx context.Context, messages []message.Message) (err error) {
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

	// Готовим VALUES для массового обновления
	// Обновляем поле status и error в зависимости от id
	// Используем конструкцию UPDATE ... FROM (VALUES ...) для разных значений

	placeholders := make([]string, 0, len(messages))
	args := make([]interface{}, 0, len(messages)*3)
	argIdx := 1

	for _, m := range messages {
		// Формируем плейсхолдеры без использования fmt.Sprintf для безопасности
		var phBuilder strings.Builder
		phBuilder.WriteString("($")
		phBuilder.WriteString(strconv.Itoa(argIdx))
		phBuilder.WriteString("::uuid, $")
		phBuilder.WriteString(strconv.Itoa(argIdx + 1))
		phBuilder.WriteString("::message_status, $")
		phBuilder.WriteString(strconv.Itoa(argIdx + 2))
		phBuilder.WriteString(")")
		placeholders = append(placeholders, phBuilder.String())
		args = append(args, m.ID, string(m.Status), m.Error)
		argIdx += 3
	}

	valuesSQL := strings.Join(placeholders, ", ")

	var qb strings.Builder
	qb.WriteString("UPDATE ")
	qb.WriteString(r.table)
	qb.WriteString(" AS m SET status = v.status, error = v.error FROM (VALUES ")
	qb.WriteString(valuesSQL)
	qb.WriteString(") AS v(id, status, error) WHERE m.id = v.id")
	query := qb.String()

	if _, execErr := tx.ExecContext(ctx, query, args...); execErr != nil {
		err = fmt.Errorf("error executing batch update: %w", execErr)
		return
	}

	if err = tx.Commit(); err != nil {
		err = fmt.Errorf("error committing update transaction: %w", err)
		return
	}

	return nil
}
