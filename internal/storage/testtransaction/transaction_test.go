package testtransaction

import (
	"db-worker/internal/storage"
	"db-worker/internal/storage/mocks"
	"db-worker/pkg/random"
	"errors"
	"testing"

	"github.com/golang/mock/gomock"
	"github.com/stretchr/testify/assert"
)

func TestNewTestTransaction(t *testing.T) {
	t.Parallel()

	err := errors.New("test error")

	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	driver := mocks.NewMockDriver(ctrl)
	failedDriver := mocks.NewMockDriver(ctrl)

	requests := map[storage.Driver]*storage.Request{
		driver: {
			Val:  "INSERT INTO users.users (user_id) VALUES ($1)",
			Args: []any{"1"},
		},
	}

	begun := map[storage.Driver]struct{}{
		driver: {},
	}

	operationHash := []byte{0x1, 0x2, 0x3}
	instanceID := 1
	id := random.String(10)
	status := string(storage.TxStatusInProgress)

	originalTx := NewTestTransaction()

	expected := &TestTransaction{
		err:           err,
		requests:      requests,
		begun:         begun,
		failedDriver:  failedDriver,
		operationHash: operationHash,
		instanceID:    instanceID,
		id:            id,
		status:        status,
		originalTx:    originalTx,
	}

	tx := NewTestTransaction(
		WithID(id),
		WithStatus(status),
		WithErr(err),
		WithRequests(requests),
		WithBegun(begun),
		WithFailedDriver(failedDriver),
		WithOperationHash(operationHash),
		WithInstanceID(instanceID),
		WithOriginalTx(originalTx),
	)

	assert.Equal(t, expected, tx)
}

func TestTestTransaction_SetFailedDriver(t *testing.T) {
	t.Parallel()

	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	driver := mocks.NewMockDriver(ctrl)

	tx := &TestTransaction{
		id:     random.String(10),
		status: string(storage.TxStatusInProgress),
		requests: map[storage.Driver]*storage.Request{
			driver: {
				Val:  "INSERT INTO users.users (user_id) VALUES ($1)",
				Args: []any{"1"},
			},
		},
	}

	tx.SetFailedDriver(driver)

	assert.Equal(t, driver, tx.FailedDriver())
}

func TestTestTransaction_Drivers(t *testing.T) {
	t.Parallel()

	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	driver := mocks.NewMockDriver(ctrl)
	driver2 := mocks.NewMockDriver(ctrl)
	driver3 := mocks.NewMockDriver(ctrl)

	tx := &TestTransaction{
		requests: map[storage.Driver]*storage.Request{
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

	expectedDrivers := []storage.Driver{
		driver,
		driver2,
		driver3,
	}

	assert.Len(t, tx.Drivers(), 3)
	assert.ElementsMatch(t, expectedDrivers, tx.Drivers())
}

func TestTestTransaction_SetStatus(t *testing.T) {
	t.Parallel()

	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	driver := mocks.NewMockDriver(ctrl)

	tx := &TestTransaction{
		id:     random.String(10),
		status: string(storage.TxStatusInProgress),
		requests: map[storage.Driver]*storage.Request{
			driver: {
				Val:  "INSERT INTO users.users (user_id) VALUES ($1)",
				Args: []any{"1"},
			},
		},
	}

	status := string(storage.TxStatusSuccess)

	tx.SetStatus(status)

	assert.Equal(t, string(status), tx.Status())
}

func TestTestTransaction_SetFailedStatus(t *testing.T) {
	t.Parallel()

	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	driver := mocks.NewMockDriver(ctrl)

	tx := &TestTransaction{
		id:     random.String(10),
		status: string(storage.TxStatusInProgress),
		requests: map[storage.Driver]*storage.Request{
			driver: {
				Val:  "INSERT INTO users.users (user_id) VALUES ($1)",
				Args: []any{"1"},
			},
		},
	}

	err := errors.New("test error")

	tx.SetFailedStatus(driver, err)

	assert.Equal(t, driver, tx.FailedDriver())
	assert.Equal(t, string(storage.TxStatusFailed), tx.Status())
	assert.Equal(t, err, tx.err)
}

func TestTestTransaction_SetSuccessStatus(t *testing.T) {
	t.Parallel()

	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	driver := mocks.NewMockDriver(ctrl)

	tx := &TestTransaction{
		id:     random.String(10),
		status: string(storage.TxStatusInProgress),
		requests: map[storage.Driver]*storage.Request{
			driver: {
				Val:  "INSERT INTO users.users (user_id) VALUES ($1)",
				Args: []any{"1"},
			},
		},
	}

	tx.SetSuccessStatus()

	assert.Equal(t, string(storage.TxStatusSuccess), tx.Status())
}

func TestTestTransaction_IsInProgress(t *testing.T) {
	t.Parallel()

	tx := &TestTransaction{
		status: string(storage.TxStatusInProgress),
	}

	assert.True(t, tx.IsInProgress())
}

func TestTestTransaction_IsFailed(t *testing.T) {
	t.Parallel()

	tx := &TestTransaction{
		status: string(storage.TxStatusFailed),
	}

	assert.True(t, tx.IsFailed())
}

//nolint:funlen // длинный тест
func TestTestTransaction_IsEqualStatus(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name   string
		tx     *TestTransaction
		status string
		want   assert.BoolAssertionFunc
	}{
		{
			name: "equal to success",
			tx: &TestTransaction{
				status: string(storage.TxStatusSuccess),
			},
			status: string(storage.TxStatusSuccess),
			want:   assert.True,
		},
		{
			name: "equal to failed",
			tx: &TestTransaction{
				status: string(storage.TxStatusFailed),
			},
			status: string(storage.TxStatusFailed),
			want:   assert.True,
		},
		{
			name: "equal to in progress",
			tx: &TestTransaction{
				status: string(storage.TxStatusInProgress),
			},
			status: string(storage.TxStatusInProgress),
			want:   assert.True,
		},
		{
			name: "not equal to success",
			tx: &TestTransaction{
				status: string(storage.TxStatusInProgress),
			},
			status: string(storage.TxStatusSuccess),
			want:   assert.False,
		},
		{
			name: "not equal to failed",
			tx: &TestTransaction{
				status: string(storage.TxStatusInProgress),
			},
			status: string(storage.TxStatusFailed),
			want:   assert.False,
		},
		{
			name: "not equal to in progress",
			tx: &TestTransaction{
				status: string(storage.TxStatusSuccess),
			},
			status: string(storage.TxStatusInProgress),
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

func TestTestTransaction_Requests(t *testing.T) {
	t.Parallel()

	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	driver := mocks.NewMockDriver(ctrl)

	tx := &TestTransaction{
		requests: map[storage.Driver]*storage.Request{
			driver: {
				Val:  "INSERT INTO users.users (user_id) VALUES ($1)",
				Args: []any{"1"},
			},
		},
	}

	expectedRequests := map[storage.Driver]*storage.Request{
		driver: {
			Val:  "INSERT INTO users.users (user_id) VALUES ($1)",
			Args: []any{"1"},
		},
	}

	assert.Equal(t, expectedRequests, tx.Requests())
}

func TestTestTransaction_SaveRequests(t *testing.T) {
	t.Parallel()

	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	driver := mocks.NewMockDriver(ctrl)

	tx := &TestTransaction{}

	requests := map[storage.Driver]*storage.Request{
		driver: {
			Val:  "INSERT INTO users.users (user_id) VALUES ($1)",
			Args: []any{"1"},
		},
	}

	tx.SaveRequests(requests)

	assert.Equal(t, requests, tx.Requests())
}

func TestTestTransaction_ID(t *testing.T) {
	t.Parallel()

	tx := &TestTransaction{
		id: random.String(10),
	}

	assert.Equal(t, tx.id, tx.ID())
}

func TestTestTransaction_Error(t *testing.T) {
	t.Parallel()

	err := errors.New("test error")

	tx := &TestTransaction{
		err: err,
	}

	assert.Equal(t, err, tx.Error())
}

func TestTestTransaction_ErrorString(t *testing.T) {
	t.Parallel()

	err := errors.New("test error")

	tx := &TestTransaction{
		err: err,
	}

	assert.Equal(t, err.Error(), tx.ErrorString())
}

func TestTestTransaction_FailedDriver(t *testing.T) {
	t.Parallel()

	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	driver := mocks.NewMockDriver(ctrl)

	tx := &TestTransaction{
		failedDriver: driver,
	}

	assert.Equal(t, driver, tx.FailedDriver())
}

func TestTestTransaction_Status(t *testing.T) {
	t.Parallel()

	tx := &TestTransaction{
		status: string(storage.TxStatusInProgress),
	}

	assert.Equal(t, string(storage.TxStatusInProgress), tx.Status())
}

func TestTestTransaction_Begun(t *testing.T) {
	t.Parallel()

	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	driver := mocks.NewMockDriver(ctrl)

	tx := &TestTransaction{
		begun: map[storage.Driver]struct{}{driver: {}},
	}

	assert.Equal(t, map[storage.Driver]struct{}{driver: {}}, tx.Begun())
}

func TestTestTransaction_AddBegunDriver(t *testing.T) {
	t.Parallel()

	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	driver := mocks.NewMockDriver(ctrl)

	tx := &TestTransaction{
		begun: map[storage.Driver]struct{}{driver: {}},
	}

	tx.AddBegunDriver(driver)

	assert.Equal(t, map[storage.Driver]struct{}{driver: {}}, tx.Begun())
}

func TestTestTransaction_InstanceID(t *testing.T) {
	t.Parallel()

	tx := &TestTransaction{
		instanceID: 1,
	}

	assert.Equal(t, 1, tx.InstanceID())
}

func TestTestTransaction_OperationHash(t *testing.T) {
	t.Parallel()

	operationHash := []byte{0x1, 0x2, 0x3}

	tx := &TestTransaction{
		operationHash: operationHash,
	}

	assert.Equal(t, operationHash, tx.OperationHash())
}

func TestTestTransaction_OriginalTx(t *testing.T) {
	t.Parallel()

	originalTx := NewTestTransaction()

	tx := &TestTransaction{
		id:         random.String(10),
		originalTx: originalTx,
	}

	assert.Equal(t, originalTx, tx.OriginalTx())
}

func TestTransaction_FailedDriverName(t *testing.T) {
	t.Parallel()

	driverName := "test-driver"

	tests := []struct {
		name string
		tx   func(driver *mocks.MockDriver) *TestTransaction
		want string
	}{
		{
			name: "positive case",
			tx: func(driver *mocks.MockDriver) *TestTransaction {
				driver.EXPECT().Name().Return(driverName)

				return &TestTransaction{
					failedDriver: driver,
				}
			},
			want: driverName,
		},
		{
			name: "negative case",
			tx: func(_ *mocks.MockDriver) *TestTransaction {
				return &TestTransaction{
					failedDriver: nil,
				}
			},
			want: "",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			ctrl := gomock.NewController(t)
			defer ctrl.Finish()

			driver := mocks.NewMockDriver(ctrl)

			assert.Equal(t, tt.want, tt.tx(driver).FailedDriverName())
		})
	}
}
