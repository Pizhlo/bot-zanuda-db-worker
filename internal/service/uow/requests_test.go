package uow

import (
	"db-worker/internal/config/operation"
	uowmocks "db-worker/internal/service/uow/mocks"
	"db-worker/internal/storage"
	"db-worker/internal/storage/mocks"
	"db-worker/internal/storage/testtransaction"
	"db-worker/pkg/random"
	"errors"
	"testing"

	"github.com/golang/mock/gomock"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

//nolint:funlen,dupl // много тест-кейсов, похожие тесты
func TestSaveRequests(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name       string
		createTx   func(t *testing.T, userDriver storage.Driver, systemDriver storage.Driver) storage.TransactionEditor
		setupMocks func(t *testing.T, driver *mocks.MockDriver, systemDriver *mocks.MockDriver)
		checkTx    func(t *testing.T, tx storage.TransactionEditor, userDriver storage.Driver)
		wantErr    require.ErrorAssertionFunc
	}{
		{
			name: "positive case",
			createTx: func(t *testing.T, userDriver storage.Driver, systemDriver storage.Driver) storage.TransactionEditor {
				t.Helper()

				origTx := testtransaction.NewTestTransaction(
					testtransaction.WithRequests(map[storage.Driver]*storage.Request{
						userDriver: {
							Val:  "INSERT INTO users.users (user_id) VALUES ($1)",
							Args: []any{"1"},
						},
					}),
				)

				return testtransaction.NewTestTransaction(
					testtransaction.WithStatus(string(storage.TxStatusInProgress)),
					testtransaction.WithRequests(map[storage.Driver]*storage.Request{
						userDriver: {
							Val:  "INSERT INTO transactions.transactions (transaction_id) VALUES ($1)",
							Args: []any{"1"},
						},
					}),
					testtransaction.WithOriginalTx(origTx),
					testtransaction.WithOperationHash([]byte{0x1, 0x2, 0x3}),
					testtransaction.WithInstanceID(1),
				)
			},
			setupMocks: func(t *testing.T, driver *mocks.MockDriver, systemDriver *mocks.MockDriver) {
				t.Helper()

				systemDriver.EXPECT().Name().Return("system-storage").AnyTimes()
				systemDriver.EXPECT().Type().Return(operation.StorageTypePostgres).AnyTimes()
				systemDriver.EXPECT().Begin(gomock.Any(), gomock.Any()).Return(nil).AnyTimes()
				systemDriver.EXPECT().Exec(gomock.Any(), gomock.Any(), gomock.Any()).Return(nil).AnyTimes()
				systemDriver.EXPECT().Commit(gomock.Any(), gomock.Any()).Return(nil).AnyTimes()
				systemDriver.EXPECT().Rollback(gomock.Any(), gomock.Any()).Return(nil).AnyTimes()
				systemDriver.EXPECT().FinishTx(gomock.Any(), gomock.Any()).Return(nil).AnyTimes()

				driver.EXPECT().Type().Return(operation.StorageTypePostgres).AnyTimes()
				driver.EXPECT().Name().Return("test-storage").AnyTimes()
			},
			checkTx: func(t *testing.T, tx storage.TransactionEditor, userDriver storage.Driver) {
				t.Helper()

				assert.Equal(t, string(storage.TxStatusInProgress), tx.Status())
			},
			wantErr: require.NoError,
		},
		{
			name: "error BuildRequests",
			createTx: func(t *testing.T, userDriver storage.Driver, systemDriver storage.Driver) storage.TransactionEditor {
				t.Helper()

				origTx := testtransaction.NewTestTransaction(
					testtransaction.WithRequests(map[storage.Driver]*storage.Request{
						userDriver: {
							Val:  "INSERT INTO users.users (user_id) VALUES ($1)",
							Args: []any{"1"},
						},
					}),
				)

				return testtransaction.NewTestTransaction(
					testtransaction.WithStatus(string(storage.TxStatusInProgress)),
					testtransaction.WithRequests(map[storage.Driver]*storage.Request{
						userDriver: {
							Val:  "INSERT INTO transactions.transactions (transaction_id) VALUES ($1)",
							Args: []any{"1"},
						},
					}),
					testtransaction.WithOriginalTx(origTx),
					testtransaction.WithOperationHash([]byte{0x1, 0x2, 0x3}),
					testtransaction.WithInstanceID(1),
				)
			},
			setupMocks: func(t *testing.T, driver *mocks.MockDriver, systemDriver *mocks.MockDriver) {
				t.Helper()

				systemDriver.EXPECT().Type().Return(operation.StorageTypeRabbitMQ).AnyTimes()

				driver.EXPECT().Type().Return(operation.StorageType("unknown")).AnyTimes()
				driver.EXPECT().Name().Return("test-storage").AnyTimes()
			},
			checkTx: func(t *testing.T, tx storage.TransactionEditor, userDriver storage.Driver) {
				t.Helper()

				assert.Equal(t, string(storage.TxStatusInProgress), tx.Status())
			},
			wantErr: require.ErrorAssertionFunc(func(t require.TestingT, err error, i ...interface{}) {
				require.Error(t, err)
				require.ErrorContains(t, err, "error get builder by storage type")
				require.ErrorContains(t, err, "error building requests for saving requests")
			}),
		},
		{
			name: "error execRequests",
			createTx: func(t *testing.T, userDriver storage.Driver, systemDriver storage.Driver) storage.TransactionEditor {
				t.Helper()

				origTx := testtransaction.NewTestTransaction(
					testtransaction.WithRequests(map[storage.Driver]*storage.Request{
						userDriver: {
							Val:  "INSERT INTO users.users (user_id) VALUES ($1)",
							Args: []any{"1"},
						},
					}),
				)

				return testtransaction.NewTestTransaction(
					testtransaction.WithStatus(string(storage.TxStatusInProgress)),
					testtransaction.WithRequests(map[storage.Driver]*storage.Request{
						userDriver: {
							Val:  "INSERT INTO transactions.transactions (transaction_id) VALUES ($1)",
							Args: []any{"1"},
						},
					}),
					testtransaction.WithOriginalTx(origTx),
					testtransaction.WithOperationHash([]byte{0x1, 0x2, 0x3}),
					testtransaction.WithInstanceID(1),
				)
			},
			setupMocks: func(t *testing.T, driver *mocks.MockDriver, systemDriver *mocks.MockDriver) {
				t.Helper()

				systemDriver.EXPECT().Name().Return("system-storage").AnyTimes()
				systemDriver.EXPECT().Type().Return(operation.StorageTypePostgres).AnyTimes()

				systemDriver.EXPECT().Begin(gomock.Any(), gomock.Any()).Return(nil).AnyTimes()
				systemDriver.EXPECT().Exec(gomock.Any(), gomock.Any(), gomock.Any()).Return(errors.New("exec error")).AnyTimes()

				driver.EXPECT().Type().Return(operation.StorageTypePostgres).AnyTimes()
				driver.EXPECT().Name().Return("test-storage").AnyTimes()

				// updateTX after fail
				systemDriver.EXPECT().Exec(gomock.Any(), gomock.Any(), gomock.Any()).Return(nil).AnyTimes()
				systemDriver.EXPECT().Commit(gomock.Any(), gomock.Any()).Return(nil).AnyTimes()
				systemDriver.EXPECT().Rollback(gomock.Any(), gomock.Any()).Return(nil).AnyTimes()
				systemDriver.EXPECT().FinishTx(gomock.Any(), gomock.Any()).Return(nil).AnyTimes()
			},
			checkTx: func(t *testing.T, tx storage.TransactionEditor, userDriver storage.Driver) {
				t.Helper()

				assert.Equal(t, string(storage.TxStatusFailed), tx.Status())
			},
			wantErr: require.ErrorAssertionFunc(func(t require.TestingT, err error, i ...interface{}) {
				require.Error(t, err)
				require.ErrorContains(t, err, "error while saving requests")
				require.ErrorContains(t, err, "error exec request")
			}),
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			ctrl := gomock.NewController(t)
			defer ctrl.Finish()

			systemDriver := mocks.NewMockDriver(ctrl)
			userDriver := mocks.NewMockDriver(ctrl)
			metricsService := uowmocks.NewMocktxCounter(ctrl)

			svc := newTestService(t, systemDriver, userDriver, metricsService)

			tt.setupMocks(t, userDriver, systemDriver)

			tx := tt.createTx(t, userDriver, systemDriver)

			err := svc.saveRequests(t.Context(), tx, tx.Requests())
			tt.wantErr(t, err)

			tt.checkTx(t, tx, userDriver)
		})
	}
}

func TestFieldsForReq(t *testing.T) {
	t.Parallel()

	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	txID := random.String(10)

	driverType, driverName := string(operation.StorageTypePostgres), "test-storage"

	expectedFields := map[string]any{
		"tx_id":       txID,
		"driver_type": driverType,
		"driver_name": driverName,
	}

	msg := fieldsForReq(txID, driverType, driverName)

	assert.Equal(t, expectedFields, msg)
}
