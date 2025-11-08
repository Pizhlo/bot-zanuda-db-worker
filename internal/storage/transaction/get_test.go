package transaction

import (
	"db-worker/internal/storage"
	"db-worker/pkg/random"
	"testing"
	"time"

	"github.com/DATA-DOG/go-sqlmock"
	"github.com/google/uuid"
	"github.com/huandu/go-sqlbuilder"
	"github.com/stretchr/testify/require"
)

//nolint:funlen // длинный тест
func TestRepo_GetAllTransactionsByFields(t *testing.T) {
	t.Parallel()

	txID := random.String(10)

	req := storage.RequestModel{
		ID:         uuid.New(),
		TxID:       txID,
		DriverType: "postgres",
		DriverName: "test-storage",
	}

	tests := []struct {
		name      string
		fields    map[string]any
		setupMock func(mock sqlmock.Sqlmock)
		want      []storage.TransactionModel
		wantErr   require.ErrorAssertionFunc
	}{
		{
			name: "positive case: get all transactions by fields succeeds",
			fields: map[string]any{
				"status": storage.TxStatusInProgress,
			},
			setupMock: func(mock sqlmock.Sqlmock) {
				t.Helper()

				createdAt := time.Date(2024, 1, 1, 0, 0, 0, 0, time.UTC)
				// Мок для основного запроса транзакций
				mock.ExpectQuery("SELECT transactions.transactions.id, status, data, error, instance_id, failed_driver, operation_hash, operation_type, created_at FROM transactions.transactions WHERE status = \\$1").
					WithArgs(storage.TxStatusInProgress).
					WillReturnRows(sqlmock.NewRows([]string{"id", "status", "data", "error", "instance_id", "failed_driver", "operation_hash", "operation_type", "created_at"}).
						AddRow(txID, storage.TxStatusInProgress, []byte(`{}`), "", 1, "", []byte("hash"), "operation", createdAt))
				// Мок для запроса requests по transaction_id
				mock.ExpectQuery("SELECT id, tx_id, driver_type, driver_name FROM transactions.requests WHERE tx_id = \\$1").
					WithArgs(txID).
					WillReturnRows(sqlmock.NewRows([]string{"id", "tx_id", "driver_type", "driver_name"}).AddRow(req.ID, req.TxID, req.DriverType, req.DriverName))
			},
			want: []storage.TransactionModel{
				{
					ID:            txID,
					Status:        storage.TxStatusInProgress,
					Data:          map[string]any{},
					Error:         "",
					InstanceID:    1,
					FailedDriver:  "",
					OperationHash: []byte("hash"),
					OperationType: "operation",
					CreatedAt:     time.Date(2024, 1, 1, 0, 0, 0, 0, time.UTC),
					Requests:      []storage.RequestModel{req},
				},
			},
			wantErr: require.NoError,
		},
		{
			name:    "negative case: fields map is empty",
			fields:  map[string]any{},
			wantErr: require.Error,
			want:    nil,
			setupMock: func(mock sqlmock.Sqlmock) {
				t.Helper()
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			db, mock, err := sqlmock.New()
			require.NoError(t, err)

			tt.setupMock(mock)

			repo := Repo{
				db: db,
			}

			txs, err := repo.GetAllTransactionsByFields(t.Context(), tt.fields)
			tt.wantErr(t, err)

			require.Equal(t, tt.want, txs)

			require.NoError(t, mock.ExpectationsWereMet())
		})
	}
}

func TestRepo_getRequestsByTransactionID(t *testing.T) {
	t.Parallel()

	txID := random.String(10)

	req := storage.RequestModel{
		ID:         uuid.New(),
		TxID:       txID,
		DriverType: "postgres",
		DriverName: "test-storage",
	}

	tests := []struct {
		name      string
		setupMock func(mock sqlmock.Sqlmock)
		want      []storage.RequestModel
		wantErr   require.ErrorAssertionFunc
	}{
		{
			name: "positive case: get all requests by transaction id succeeds",
			setupMock: func(mock sqlmock.Sqlmock) {
				t.Helper()

				// Мок для запроса requests по transaction_id
				mock.ExpectQuery("SELECT id, tx_id, driver_type, driver_name FROM transactions.requests WHERE tx_id = \\$1").
					WithArgs(txID).
					WillReturnRows(sqlmock.NewRows([]string{"id", "tx_id", "driver_type", "driver_name"}).AddRow(req.ID, req.TxID, req.DriverType, req.DriverName))
			},
			want:    []storage.RequestModel{req},
			wantErr: require.NoError,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			db, mock, err := sqlmock.New()
			require.NoError(t, err)

			tt.setupMock(mock)

			repo := Repo{
				db: db,
			}

			txs, err := repo.getRequestsByTransactionID(t.Context(), txID)
			tt.wantErr(t, err)

			require.Equal(t, tt.want, txs)

			require.NoError(t, mock.ExpectationsWereMet())
		})
	}
}

func TestBuildWhereConditions(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name   string
		fields map[string]any
		want   []string
	}{
		{
			name: "positive case: build where conditions succeeds",
			fields: map[string]any{
				"status":         storage.TxStatusInProgress,
				"instance_id":    1,
				"operation_type": "operation",
			},
			want: []string{
				"$1",
				"$2",
				"$3",
			},
		},
		{
			name: "positive case: build where conditions succeeds with nil value",
			fields: map[string]any{
				"status":         storage.TxStatusInProgress,
				"instance_id":    1,
				"operation_type": nil,
			},
			want: []string{
				"$1",
				"$2",
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			sb := sqlbuilder.NewSelectBuilder()
			sb.SetFlavor(sqlbuilder.PostgreSQL)
			conditions := buildWhereConditions(sb, tt.fields)
			require.Equal(t, tt.want, conditions)
		})
	}
}
