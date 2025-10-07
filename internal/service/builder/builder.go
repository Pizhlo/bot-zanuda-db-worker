// builder - пакет для создания запросов к хранилищам.
package builder

import (
	"db-worker/internal/config/operation"
	"db-worker/internal/storage"
)

// ForPostgres создает новый экземпляр query builder для PostgreSQL.
func ForPostgres() Builder {
	return &postgresBuilder{}
}

// ForRabbitMQ создает новый экземпляр query builder для RabbitMQ.
// В будущем будет реализован.
func ForRabbitMQ() Builder {
	return nil
}

// ForTypesense создает новый экземпляр query builder для Typesense.
// В будущем будет реализован.
func ForTypesense() Builder {
	return nil
}

// Builder - интерфейс для создания запросов.
// Пока что поддерживается только PostgreSQL и операции create.
type Builder interface {
	// Build формирует запрос на основе заранее переданных данных.
	Build() (*storage.Request, error)
	// WithOperation устанавливает операцию.
	WithOperation(operation operation.Operation) Builder
	// WithValues устанавливает значения.
	WithValues(vals map[string]any) Builder
	// WithTable устанавливает название таблицы.
	WithTable(table string) Builder

	operations
}

type operations interface {
	// WithCreateOperation устанавливает операцию создания.
	WithCreateOperation() Builder
}
