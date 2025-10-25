package storage

import (
	"db-worker/internal/storage/mocks"
	"db-worker/pkg/random"
	"errors"
	"testing"

	"github.com/golang/mock/gomock"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

//nolint:funlen // длинный тест
func TestNewTransaction(t *testing.T) {
	t.Parallel()

	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	driver := mocks.NewMockDriver(ctrl)

	tests := []struct {
		name          string
		requests      map[Driver]*Request
		instanceID    int
		operationHash []byte
		want          *Transaction
		wantError     require.ErrorAssertionFunc
		checkWant     func(want, got *Transaction)
	}{
		{
			name: "positive case",
			requests: map[Driver]*Request{
				driver: {
					Val:  "insert into users.users (id, name) values ($1, $2)",
					Args: []any{1, "ivan"},
					Raw: map[string]any{
						"id":   1,
						"name": "ivan",
					},
				},
			},
			instanceID:    1,
			operationHash: []byte{0x1, 0x2, 0x3},
			want: &Transaction{
				status: TxStatusInProgress,
				requests: map[Driver]*Request{
					driver: {
						Val:  "insert into users.users (id, name) values ($1, $2)",
						Args: []any{1, "ivan"},
						Raw: map[string]any{
							"id":   1,
							"name": "ivan",
						},
					},
				},
				instanceID:    1,
				operationHash: []byte{0x1, 0x2, 0x3},
			},
			checkWant: func(want, got *Transaction) {
				assert.Equal(t, string(TxStatusInProgress), got.Status())
				assert.EqualValues(t, want.Requests(), got.Requests())
				assert.Equal(t, want.InstanceID(), got.InstanceID())
				assert.Equal(t, want.OperationHash(), got.OperationHash())

				assert.Len(t, got.begun, 0)
				assert.Len(t, got.ID(), 10)
			},
			wantError: require.NoError,
		},
		{
			name:          "requests not provided",
			instanceID:    1,
			operationHash: []byte{0x1, 0x2, 0x3},
			wantError:     require.Error,
		},
		{
			name:       "operation hash not provided",
			instanceID: 1,
			requests: map[Driver]*Request{
				driver: {
					Val:  "insert into users.users (id, name) values ($1, $2)",
					Args: []any{1, "ivan"},
					Raw: map[string]any{
						"id":   1,
						"name": "ivan",
					},
				},
			},
			wantError: require.Error,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			tx, err := NewTransaction(tt.requests, tt.instanceID, tt.operationHash)
			tt.wantError(t, err)

			if tt.checkWant != nil {
				tt.checkWant(tt.want, tx)
			}
		})
	}
}

func TestTransaction_SetFailedDriver(t *testing.T) {
	t.Parallel()

	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	driver := mocks.NewMockDriver(ctrl)

	tx := &Transaction{
		id:     random.String(10),
		status: TxStatusInProgress,
		requests: map[Driver]*Request{
			driver: {
				Val:  "INSERT INTO users.users (user_id) VALUES ($1)",
				Args: []any{"1"},
			},
		},
	}

	tx.SetFailedDriver(driver)

	assert.Equal(t, driver, tx.FailedDriver())
}

func TestTransaction_Drivers(t *testing.T) {
	t.Parallel()

	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	driver := mocks.NewMockDriver(ctrl)
	driver2 := mocks.NewMockDriver(ctrl)
	driver3 := mocks.NewMockDriver(ctrl)

	tx := &Transaction{
		requests: map[Driver]*Request{
			driver: {
				Val:  "INSERT INTO users.users (user_id) VALUES ($1)",
				Args: []any{"1"},
			},
			driver2: {
				Val:  "INSERT INTO users.users (user_id) VALUES ($1)",
				Args: []any{"1"},
			},
			driver3: {
				Val:  "INSERT INTO users.users (user_id) VALUES ($1)",
				Args: []any{"1"},
			},
		},
	}

	expectedDrivers := []Driver{
		driver,
		driver2,
		driver3,
	}

	assert.Len(t, tx.Drivers(), 3)
	assert.Equal(t, expectedDrivers, tx.Drivers())
}

func TestTransaction_SetStatus(t *testing.T) {
	t.Parallel()

	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	driver := mocks.NewMockDriver(ctrl)

	tx := &Transaction{
		id:     random.String(10),
		status: TxStatusInProgress,
		requests: map[Driver]*Request{
			driver: {
				Val:  "INSERT INTO users.users (user_id) VALUES ($1)",
				Args: []any{"1"},
			},
		},
	}

	status := TxStatusSuccess

	tx.SetStatus(status)

	assert.Equal(t, string(status), tx.Status())
}

func TestTransaction_SetFailedStatus(t *testing.T) {
	t.Parallel()

	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	driver := mocks.NewMockDriver(ctrl)

	tx := &Transaction{
		id:     random.String(10),
		status: TxStatusInProgress,
		requests: map[Driver]*Request{
			driver: {
				Val:  "INSERT INTO users.users (user_id) VALUES ($1)",
				Args: []any{"1"},
			},
		},
	}

	err := errors.New("test error")

	tx.SetFailedStatus(driver, err)

	assert.Equal(t, driver, tx.FailedDriver())
	assert.Equal(t, string(TxStatusFailed), tx.Status())
	assert.Equal(t, err, tx.err)
}

func TestTransaction_SetSuccessStatus(t *testing.T) {
	t.Parallel()

	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	driver := mocks.NewMockDriver(ctrl)

	tx := &Transaction{
		id:     random.String(10),
		status: TxStatusInProgress,
		requests: map[Driver]*Request{
			driver: {
				Val:  "INSERT INTO users.users (user_id) VALUES ($1)",
				Args: []any{"1"},
			},
		},
	}

	tx.SetSuccessStatus()

	assert.Equal(t, string(TxStatusSuccess), tx.Status())
}

func TestTransaction_IsInProgress(t *testing.T) {
	t.Parallel()

	tx := &Transaction{
		status: TxStatusInProgress,
	}

	assert.True(t, tx.IsInProgress())
}

func TestTransaction_IsFailed(t *testing.T) {
	t.Parallel()

	tx := &Transaction{
		status: TxStatusFailed,
	}

	assert.True(t, tx.IsFailed())
}

//nolint:funlen // длинный тест
func TestTransaction_IsEqualStatus(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name   string
		tx     *Transaction
		status txStatus
		want   assert.BoolAssertionFunc
	}{
		{
			name: "equal to success",
			tx: &Transaction{
				status: TxStatusSuccess,
			},
			status: TxStatusSuccess,
			want:   assert.True,
		},
		{
			name: "equal to failed",
			tx: &Transaction{
				status: TxStatusFailed,
			},
			status: TxStatusFailed,
			want:   assert.True,
		},
		{
			name: "equal to in progress",
			tx: &Transaction{
				status: TxStatusInProgress,
			},
			status: TxStatusInProgress,
			want:   assert.True,
		},
		{
			name: "not equal to success",
			tx: &Transaction{
				status: TxStatusInProgress,
			},
			status: TxStatusSuccess,
			want:   assert.False,
		},
		{
			name: "not equal to failed",
			tx: &Transaction{
				status: TxStatusInProgress,
			},
			status: TxStatusFailed,
			want:   assert.False,
		},
		{
			name: "not equal to in progress",
			tx: &Transaction{
				status: TxStatusSuccess,
			},
			status: TxStatusInProgress,
			want:   assert.False,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			tt.want(t, tt.tx.isEqualStatus(tt.status))
		})
	}
}

func TestTransaction_Requests(t *testing.T) {
	t.Parallel()

	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	driver := mocks.NewMockDriver(ctrl)

	tx := &Transaction{
		requests: map[Driver]*Request{
			driver: {
				Val:  "INSERT INTO users.users (user_id) VALUES ($1)",
				Args: []any{"1"},
			},
		},
	}

	expectedRequests := map[Driver]*Request{
		driver: {
			Val:  "INSERT INTO users.users (user_id) VALUES ($1)",
			Args: []any{"1"},
		},
	}

	assert.Equal(t, expectedRequests, tx.Requests())
}

func TestTransaction_SaveRequests(t *testing.T) {
	t.Parallel()

	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	driver := mocks.NewMockDriver(ctrl)

	tx := &Transaction{}

	requests := map[Driver]*Request{
		driver: {
			Val:  "INSERT INTO users.users (user_id) VALUES ($1)",
			Args: []any{"1"},
		},
	}

	tx.SaveRequests(requests)

	assert.NotEqual(t, requests, tx.Requests())
	assert.Empty(t, tx.Requests())
}

func TestTransaction_ID(t *testing.T) {
	t.Parallel()

	tx := &Transaction{
		id: random.String(10),
	}

	assert.Equal(t, tx.id, tx.ID())
}

func TestTransaction_Error(t *testing.T) {
	t.Parallel()

	err := errors.New("test error")

	tx := &Transaction{
		err: err,
	}

	assert.Equal(t, err, tx.Error())
}

func TestTransaction_ErrorString(t *testing.T) {
	t.Parallel()

	err := errors.New("test error")

	tx := &Transaction{
		err: err,
	}

	assert.Equal(t, err.Error(), tx.ErrorString())
}

func TestTransaction_FailedDriver(t *testing.T) {
	t.Parallel()

	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	driver := mocks.NewMockDriver(ctrl)

	tx := &Transaction{
		failedDriver: driver,
	}

	assert.Equal(t, driver, tx.FailedDriver())
}

func TestTransaction_Status(t *testing.T) {
	t.Parallel()

	tx := &Transaction{
		status: TxStatusInProgress,
	}

	assert.Equal(t, string(TxStatusInProgress), tx.Status())
}

func TestTransaction_Begun(t *testing.T) {
	t.Parallel()

	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	driver := mocks.NewMockDriver(ctrl)

	tx := &Transaction{
		begun: map[Driver]struct{}{driver: {}},
	}

	assert.Equal(t, map[Driver]struct{}{driver: {}}, tx.Begun())
}

func TestTransaction_AddBegunDriver(t *testing.T) {
	t.Parallel()

	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	driver := mocks.NewMockDriver(ctrl)

	tx := &Transaction{
		begun: map[Driver]struct{}{driver: {}},
	}

	tx.AddBegunDriver(driver)

	assert.Equal(t, map[Driver]struct{}{driver: {}}, tx.Begun())
}

func TestTransaction_InstanceID(t *testing.T) {
	t.Parallel()

	tx := &Transaction{
		instanceID: 1,
	}

	assert.Equal(t, 1, tx.InstanceID())
}

func TestTransaction_OperationHash(t *testing.T) {
	t.Parallel()

	operationHash := []byte{0x1, 0x2, 0x3}

	tx := &Transaction{
		operationHash: operationHash,
	}

	assert.Equal(t, operationHash, tx.OperationHash())
}

func TestTransaction_OriginalTx(t *testing.T) {
	t.Parallel()

	tx := &Transaction{
		id: random.String(10),
	}

	assert.Equal(t, tx, tx.OriginalTx())
}

func TestTransaction_FailedDriverName(t *testing.T) {
	t.Parallel()

	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	driverName := "test-driver"

	tests := []struct {
		name string
		tx   func() *Transaction
		want string
	}{
		{
			name: "positive case",
			tx: func() *Transaction {
				driver := mocks.NewMockDriver(ctrl)
				driver.EXPECT().Name().Return(driverName)

				return &Transaction{
					failedDriver: driver,
				}
			},
			want: driverName,
		},
		{
			name: "negative case",
			tx: func() *Transaction {
				return &Transaction{
					failedDriver: nil,
				}
			},
			want: "",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			assert.Equal(t, tt.want, tt.tx().FailedDriverName())
		})
	}
}

//nolint:funlen // длинный тест
func TestNewUtilityTransaction(t *testing.T) {
	t.Parallel()

	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	driver := mocks.NewMockDriver(ctrl)

	randomID := random.String(10)

	tests := []struct {
		name      string
		options   []option
		wantError require.ErrorAssertionFunc
		want      *utilityTransaction
		checkWant func(want, got *utilityTransaction)
	}{
		{
			name: "positive case",
			options: []option{
				WithDriver(driver),
				WithOriginalTx(&Transaction{
					id:     randomID,
					status: TxStatusInProgress,
					requests: map[Driver]*Request{
						driver: {
							Val:  "insert into users.users (id, name) values ($1, $2)",
							Args: []any{1, "ivan"},
							Raw: map[string]any{
								"id":   1,
								"name": "ivan",
							},
						},
					},
					instanceID:    1,
					operationHash: []byte{0x1, 0x2, 0x3},
				}),
			},
			want: &utilityTransaction{
				Transaction: Transaction{
					begun:  make(map[Driver]struct{}),
					err:    ErrEmpty,
					status: TxStatusInProgress,
				},
				originalTx: &Transaction{
					id:     randomID,
					status: TxStatusInProgress,
					requests: map[Driver]*Request{
						driver: {
							Val:  "insert into users.users (id, name) values ($1, $2)",
							Args: []any{1, "ivan"},
							Raw: map[string]any{
								"id":   1,
								"name": "ivan",
							},
						},
					},
					instanceID:    1,
					operationHash: []byte{0x1, 0x2, 0x3},
				},
				requests: make(map[Driver]*Request),
				drivers:  []Driver{driver},
			},
			checkWant: func(want, got *utilityTransaction) {
				assert.Equal(t, want, got)
			},
			wantError: require.NoError,
		},
		{
			name: "negative case: no drivers provided",
			options: []option{
				WithOriginalTx(&Transaction{
					id:     randomID,
					status: TxStatusInProgress,
				}),
			},
			wantError: require.Error,
		},
		{
			name: "negative case: original transaction not provided",
			options: []option{
				WithDriver(driver),
			},
			wantError: require.Error,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			tx, err := NewUtilityTransaction(tt.options...)
			tt.wantError(t, err)

			if tt.checkWant != nil {
				tt.checkWant(tt.want, tx)
			}
		})
	}
}

func TestUtilityTransaction_SetFailedDriver(t *testing.T) {
	t.Parallel()

	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	driver := mocks.NewMockDriver(ctrl)

	tx := &utilityTransaction{
		originalTx: &Transaction{
			id:     random.String(10),
			status: TxStatusInProgress,
			requests: map[Driver]*Request{
				driver: {
					Val:  "INSERT INTO users.users (user_id) VALUES ($1)",
					Args: []any{"1"},
				},
			},
		},
		Transaction: Transaction{
			id:     random.String(10),
			status: TxStatusInProgress,
			requests: map[Driver]*Request{
				driver: {
					Val:  "INSERT INTO users.users (user_id) VALUES ($1)",
					Args: []any{"1"},
				},
			},
		},
	}

	tx.SetFailedDriver(driver)

	assert.Equal(t, driver, tx.FailedDriver())
}

func TestUtilityTransaction_Drivers(t *testing.T) {
	t.Parallel()

	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	driver := mocks.NewMockDriver(ctrl)
	driver2 := mocks.NewMockDriver(ctrl)
	driver3 := mocks.NewMockDriver(ctrl)

	tx := &utilityTransaction{
		drivers: []Driver{
			driver,
			driver2,
			driver3,
		},
	}

	expectedDrivers := []Driver{
		driver,
		driver2,
		driver3,
	}

	assert.Len(t, tx.Drivers(), 3)
	assert.Equal(t, expectedDrivers, tx.Drivers())
}

func TestUtilityTransaction_SetStatus(t *testing.T) {
	t.Parallel()

	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	driver := mocks.NewMockDriver(ctrl)

	tx := &utilityTransaction{
		Transaction: Transaction{
			id:     random.String(10),
			status: TxStatusInProgress,
			requests: map[Driver]*Request{
				driver: {
					Val:  "INSERT INTO users.users (user_id) VALUES ($1)",
					Args: []any{"1"},
				},
			},
		},
	}

	status := TxStatusSuccess

	tx.SetStatus(status)

	assert.Equal(t, string(status), tx.Status())
}

func TestUtilityTransaction_SetFailedStatus(t *testing.T) {
	t.Parallel()

	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	driver := mocks.NewMockDriver(ctrl)

	tx := &utilityTransaction{
		Transaction: Transaction{
			id:     random.String(10),
			status: TxStatusInProgress,
			requests: map[Driver]*Request{
				driver: {
					Val:  "INSERT INTO users.users (user_id) VALUES ($1)",
					Args: []any{"1"},
				},
			},
		},
		originalTx: &Transaction{
			id:     random.String(10),
			status: TxStatusInProgress,
			requests: map[Driver]*Request{
				driver: {
					Val:  "INSERT INTO users.users (user_id) VALUES ($1)",
					Args: []any{"1"},
				},
			},
		},
	}

	err := errors.New("test error")

	tx.SetFailedStatus(driver, err)

	assert.Equal(t, driver, tx.FailedDriver())
	assert.Equal(t, string(TxStatusFailed), tx.Status())
	assert.Equal(t, err, tx.err)
}

func TestUtilityTransaction_SetSuccessStatus(t *testing.T) {
	t.Parallel()

	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	driver := mocks.NewMockDriver(ctrl)

	tx := &utilityTransaction{
		Transaction: Transaction{
			id:     random.String(10),
			status: TxStatusInProgress,
			requests: map[Driver]*Request{
				driver: {
					Val:  "INSERT INTO users.users (user_id) VALUES ($1)",
					Args: []any{"1"},
				},
			},
		},
	}

	tx.SetSuccessStatus()

	assert.Equal(t, string(TxStatusSuccess), tx.Status())
}

func TestUtilityTransaction_IsInProgress(t *testing.T) {
	t.Parallel()

	tx := &utilityTransaction{
		Transaction: Transaction{
			status: TxStatusInProgress,
		},
	}

	assert.True(t, tx.IsInProgress())
}

func TestUtilityTransaction_IsFailed(t *testing.T) {
	t.Parallel()

	tx := &utilityTransaction{
		Transaction: Transaction{
			status: TxStatusFailed,
		},
	}

	assert.True(t, tx.IsFailed())
}

//nolint:funlen // длинный тест
func TestUtilityTransaction_IsEqualStatus(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name   string
		tx     *utilityTransaction
		status txStatus
		want   assert.BoolAssertionFunc
	}{
		{
			name: "equal to success",
			tx: &utilityTransaction{
				Transaction: Transaction{
					status: TxStatusSuccess,
				},
			},
			status: TxStatusSuccess,
			want:   assert.True,
		},
		{
			name: "equal to failed",
			tx: &utilityTransaction{
				Transaction: Transaction{
					status: TxStatusFailed,
				},
			},
			status: TxStatusFailed,
			want:   assert.True,
		},
		{
			name: "equal to in progress",
			tx: &utilityTransaction{
				Transaction: Transaction{
					status: TxStatusInProgress,
				},
			},
			status: TxStatusInProgress,
			want:   assert.True,
		},
		{
			name: "not equal to success",
			tx: &utilityTransaction{
				Transaction: Transaction{
					status: TxStatusSuccess,
				},
			},
			status: TxStatusInProgress,
			want:   assert.False,
		},
		{
			name: "not equal to failed",
			tx: &utilityTransaction{
				Transaction: Transaction{
					status: TxStatusInProgress,
				},
			},
			status: TxStatusFailed,
			want:   assert.False,
		},
		{
			name: "not equal to in progress",
			tx: &utilityTransaction{
				Transaction: Transaction{
					status: TxStatusSuccess,
				},
			},
			status: TxStatusInProgress,
			want:   assert.False,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			tt.want(t, tt.tx.isEqualStatus(tt.status))
		})
	}
}

func TestUtilityTransaction_Requests(t *testing.T) {
	t.Parallel()

	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	driver := mocks.NewMockDriver(ctrl)

	tx := &utilityTransaction{
		requests: map[Driver]*Request{
			driver: {
				Val:  "INSERT INTO users.users (user_id) VALUES ($1)",
				Args: []any{"1"},
			},
		},
	}

	expectedRequests := map[Driver]*Request{
		driver: {
			Val:  "INSERT INTO users.users (user_id) VALUES ($1)",
			Args: []any{"1"},
		},
	}

	assert.Equal(t, expectedRequests, tx.Requests())
}

func TestUtilityTransaction_SaveRequests(t *testing.T) {
	t.Parallel()

	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	driver := mocks.NewMockDriver(ctrl)

	tx := &utilityTransaction{
		Transaction: Transaction{
			requests: map[Driver]*Request{
				driver: {
					Val:  "INSERT INTO users.users (user_id) VALUES ($1)",
					Args: []any{"1"},
				},
			},
		},
	}

	requests := map[Driver]*Request{
		driver: {
			Val:  "INSERT INTO users.users (user_id) VALUES ($1)",
			Args: []any{"1"},
		},
	}

	tx.SaveRequests(requests)
}

func TestUtilityTransaction_ID(t *testing.T) {
	t.Parallel()

	tx := &utilityTransaction{
		originalTx: &Transaction{
			id: random.String(10),
		},
	}

	assert.Equal(t, tx.OriginalTx().ID(), tx.ID())
}

func TestUtilityTransaction_Error(t *testing.T) {
	t.Parallel()

	err := errors.New("test error")

	tx := &utilityTransaction{
		Transaction: Transaction{
			err: err,
		},
	}

	assert.Equal(t, err, tx.Error())
}

func TestUtilityTransaction_ErrorString(t *testing.T) {
	t.Parallel()

	err := errors.New("test error")

	tx := &utilityTransaction{
		Transaction: Transaction{
			err: err,
		},
	}

	assert.Equal(t, err.Error(), tx.ErrorString())
}

func TestUtilityTransaction_FailedDriver(t *testing.T) {
	t.Parallel()

	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	driver := mocks.NewMockDriver(ctrl)

	tx := &utilityTransaction{
		originalTx: &Transaction{},
		Transaction: Transaction{
			failedDriver: driver,
		},
	}

	assert.Equal(t, driver, tx.FailedDriver())
}

func TestUtilityTransaction_Status(t *testing.T) {
	t.Parallel()

	tx := &utilityTransaction{
		Transaction: Transaction{
			status: TxStatusInProgress,
		},
	}

	assert.Equal(t, string(TxStatusInProgress), tx.Status())
}

func TestUtilityTransaction_Begun(t *testing.T) {
	t.Parallel()

	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	driver := mocks.NewMockDriver(ctrl)

	tx := &utilityTransaction{
		Transaction: Transaction{
			begun: map[Driver]struct{}{driver: {}},
		},
	}

	assert.Equal(t, map[Driver]struct{}{driver: {}}, tx.Begun())
}

func TestUtilityTransaction_AddBegunDriver(t *testing.T) {
	t.Parallel()

	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	driver := mocks.NewMockDriver(ctrl)

	tx := &utilityTransaction{
		Transaction: Transaction{
			begun: map[Driver]struct{}{driver: {}},
		},
	}

	tx.AddBegunDriver(driver)

	assert.Equal(t, map[Driver]struct{}{driver: {}}, tx.Begun())
}

func TestUtilityTransaction_InstanceID(t *testing.T) {
	t.Parallel()

	tx := &utilityTransaction{
		Transaction: Transaction{
			instanceID: 1,
		},
		originalTx: &Transaction{
			instanceID: 1,
		},
	}

	assert.Equal(t, 1, tx.InstanceID())
}

func TestUtilityTransaction_OperationHash(t *testing.T) {
	t.Parallel()

	operationHash := []byte{0x1, 0x2, 0x3}

	tx := &utilityTransaction{
		Transaction: Transaction{
			operationHash: operationHash,
		},
		originalTx: &Transaction{},
	}

	assert.Equal(t, operationHash, tx.OperationHash())
}

func TestUtilityTransaction_OriginalTx(t *testing.T) {
	t.Parallel()

	originalTx := &Transaction{
		id: random.String(10),
	}

	tx := &utilityTransaction{
		originalTx: originalTx,
	}

	assert.Equal(t, originalTx, tx.OriginalTx())
}

func TestUtilityTransaction_FailedDriverName(t *testing.T) {
	t.Parallel()

	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	driverName := "test-driver"

	tests := []struct {
		name string
		tx   func() *utilityTransaction
		want string
	}{
		{
			name: "positive case",
			tx: func() *utilityTransaction {
				driver := mocks.NewMockDriver(ctrl)
				driver.EXPECT().Name().Return(driverName)

				return &utilityTransaction{
					originalTx: &Transaction{
						failedDriver: driver,
					},
				}
			},
			want: driverName,
		},
		{
			name: "negative case",
			tx: func() *utilityTransaction {
				return &utilityTransaction{
					originalTx: &Transaction{
						failedDriver: nil,
					},
				}
			},
			want: "",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			assert.Equal(t, tt.want, tt.tx().FailedDriverName())
		})
	}
}
