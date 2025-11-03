package message

import (
	"db-worker/internal/service/operation/message"
	"errors"
	"testing"

	"github.com/DATA-DOG/go-sqlmock"
	"github.com/google/uuid"
	"github.com/stretchr/testify/require"
)

//nolint:funlen,dupl // много тест-кейсов, одинаковые тест-кейсы для create и update
func TestRepo_CreateMany(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name      string
		setupMock func(mock sqlmock.Sqlmock)
		messages  []message.Message
		wantErr   require.ErrorAssertionFunc
	}{
		{
			name: "positive case: create one message",
			setupMock: func(mock sqlmock.Sqlmock) {
				t.Helper()

				mock.ExpectBegin()
				mock.ExpectExec(`INSERT INTO messages \(id, data, status, error, driver_type, driver_name, instance_id, operation_hash\)\s+VALUES\s+\(\$1, \$2, \$3, \$4, \$5, \$6, \$7, \$8\)`).
					WillReturnResult(sqlmock.NewResult(1, 1))
				mock.ExpectCommit()
			},
			messages: []message.Message{
				{
					ID: uuid.New(),
					Data: map[string]interface{}{
						"test": "data",
					},
					Status:        message.StatusInProgress,
					Error:         "",
					DriverType:    "test-driver",
					DriverName:    "test-driver-name",
					InstanceID:    1,
					OperationHash: []byte{1, 2, 3, 4},
				},
			},
			wantErr: require.NoError,
		},
		{
			name: "positive case: create many messages",
			setupMock: func(mock sqlmock.Sqlmock) {
				t.Helper()

				mock.ExpectBegin()
				mock.ExpectExec(`INSERT INTO messages \(id, data, status, error, driver_type, driver_name, instance_id, operation_hash\)\s+VALUES\s+\(\$1, \$2, \$3, \$4, \$5, \$6, \$7, \$8\)`).
					WillReturnResult(sqlmock.NewResult(1, 2))
				mock.ExpectCommit()
			},
			messages: []message.Message{
				{
					ID: uuid.New(),
					Data: map[string]interface{}{
						"test": "data",
					},
					Status:        message.StatusInProgress,
					Error:         "",
					DriverType:    "test-driver",
					DriverName:    "test-driver-name",
					InstanceID:    1,
					OperationHash: []byte{1, 2, 3, 4},
				},
				{
					ID: uuid.New(),
					Data: map[string]interface{}{
						"test": "data",
					},
					Status:        message.StatusInProgress,
					Error:         "",
					DriverType:    "test-driver",
					DriverName:    "test-driver-name",
					InstanceID:    1,
					OperationHash: []byte{1, 2, 3, 4},
				},
			},
			wantErr: require.NoError,
		},
		{
			name: "positive case: empty messages",
			setupMock: func(mock sqlmock.Sqlmock) {
				t.Helper()
			},
			messages: []message.Message{},
			wantErr:  require.NoError,
		},
		{
			name: "negative case: error beginning transaction",
			setupMock: func(mock sqlmock.Sqlmock) {
				t.Helper()

				mock.ExpectBegin().WillReturnError(errors.New("begin error"))
			},
			messages: []message.Message{
				{
					ID: uuid.New(),
					Data: map[string]interface{}{
						"test": "data",
					},
					Status:        message.StatusInProgress,
					Error:         "",
					DriverType:    "test-driver",
					DriverName:    "test-driver-name",
					InstanceID:    1,
					OperationHash: []byte{1, 2, 3, 4},
				},
				{
					ID: uuid.New(),
					Data: map[string]interface{}{
						"test": "data",
					},
					Status:        message.StatusInProgress,
					Error:         "",
					DriverType:    "test-driver",
					DriverName:    "test-driver-name",
					InstanceID:    1,
					OperationHash: []byte{1, 2, 3, 4},
				},
			},
			wantErr: require.ErrorAssertionFunc(func(tt require.TestingT, err error, i ...interface{}) {
				require.Error(t, err)
				require.ErrorContains(t, err, "begin error")
			}),
		},
		{
			name: "negative case: error exec",
			setupMock: func(mock sqlmock.Sqlmock) {
				t.Helper()

				mock.ExpectBegin()
				mock.ExpectExec(`INSERT INTO messages \(id, data, status, error, driver_type, driver_name, instance_id, operation_hash\)\s+VALUES\s+\(\$1, \$2, \$3, \$4, \$5, \$6, \$7, \$8\)`).
					WillReturnError(errors.New("exec error"))

				mock.ExpectRollback()
			},
			messages: []message.Message{
				{
					ID: uuid.New(),
					Data: map[string]interface{}{
						"test": "data",
					},
					Status:        message.StatusInProgress,
					Error:         "",
					DriverType:    "test-driver",
					DriverName:    "test-driver-name",
					InstanceID:    1,
					OperationHash: []byte{1, 2, 3, 4},
				},
				{
					ID: uuid.New(),
					Data: map[string]interface{}{
						"test": "data",
					},
					Status:        message.StatusInProgress,
					Error:         "",
					DriverType:    "test-driver",
					DriverName:    "test-driver-name",
					InstanceID:    1,
					OperationHash: []byte{1, 2, 3, 4},
				},
			},
			wantErr: require.ErrorAssertionFunc(func(tt require.TestingT, err error, i ...interface{}) {
				require.Error(tt, err)
				require.ErrorContains(tt, err, "exec error")
			}),
		},
		{
			name: "negative case: error commit",
			setupMock: func(mock sqlmock.Sqlmock) {
				t.Helper()

				mock.ExpectBegin()
				mock.ExpectExec(`INSERT INTO messages \(id, data, status, error, driver_type, driver_name, instance_id, operation_hash\)\s+VALUES\s+\(\$1, \$2, \$3, \$4, \$5, \$6, \$7, \$8\)`).
					WillReturnResult(sqlmock.NewResult(1, 1))
				mock.ExpectCommit().WillReturnError(errors.New("commit error"))
				// Примечание: sqlmock не регистрирует Rollback() после ошибки Commit(),
				// хотя код вызывает его в defer функции. Это ограничение sqlmock.
				// В реальной БД PostgreSQL при ошибке Commit() транзакция остается открытой
				// и должна быть откачена, что и делает наш код.
			},
			messages: []message.Message{
				{
					ID: uuid.New(),
					Data: map[string]interface{}{
						"test": "data",
					},
					Status:        message.StatusInProgress,
					Error:         "",
					DriverType:    "test-driver",
					DriverName:    "test-driver-name",
					InstanceID:    1,
					OperationHash: []byte{1, 2, 3, 4},
				},
				{
					ID: uuid.New(),
					Data: map[string]interface{}{
						"test": "data",
					},
					Status:        message.StatusInProgress,
					Error:         "",
					DriverType:    "test-driver",
					DriverName:    "test-driver-name",
					InstanceID:    1,
					OperationHash: []byte{1, 2, 3, 4},
				},
			},
			wantErr: require.ErrorAssertionFunc(func(tt require.TestingT, err error, i ...interface{}) {
				require.Error(tt, err)
				require.ErrorContains(tt, err, "commit error")
			}),
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			db, mock, err := sqlmock.New()
			require.NoError(t, err)

			repo := &Repo{
				db:    db,
				table: "messages",
			}

			tt.setupMock(mock)

			err = repo.CreateMany(t.Context(), tt.messages)
			tt.wantErr(t, err)

			require.NoError(t, mock.ExpectationsWereMet())

			mock.ExpectClose()
			require.NoError(t, db.Close())
		})
	}
}
