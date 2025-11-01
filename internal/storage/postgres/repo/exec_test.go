package postgres

import (
	"database/sql"
	"db-worker/internal/storage"
	"errors"
	"sync"
	"testing"
	"time"

	"github.com/DATA-DOG/go-sqlmock"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

//nolint:funlen // это тест
func TestRepo_Begin(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name      string
		txID      string
		setupMock func(mock sqlmock.Sqlmock)
		assertTx  func(t *testing.T, repo *Repo, txID string)
		wantErr   require.ErrorAssertionFunc
	}{
		{
			name: "positive case: begin transaction succeeds",
			txID: "test-tx-1",
			setupMock: func(mock sqlmock.Sqlmock) {
				mock.ExpectBegin()
			},
			assertTx: func(t *testing.T, repo *Repo, txID string) {
				t.Helper()

				tx, getErr := repo.getTx(txID)
				require.NoError(t, getErr)
				assert.NotNil(t, tx)
			},
			wantErr: require.NoError,
		},
		{
			name: "positive case: transaction already exists",
			txID: "test-tx-1",
			setupMock: func(mock sqlmock.Sqlmock) {
				mock.ExpectBegin()
			},
			assertTx: func(t *testing.T, repo *Repo, txID string) {
				t.Helper()

				tx, getErr := repo.getTx(txID)
				require.NoError(t, getErr)
				assert.NotNil(t, tx)
			},
			wantErr: require.NoError,
		},
		{
			name: "negative case: begin transaction fails",
			txID: "test-tx-2",
			setupMock: func(mock sqlmock.Sqlmock) {
				mock.ExpectBegin().WillReturnError(errors.New("could not begin transaction"))
				mock.ExpectClose()
			},
			wantErr: func(t require.TestingT, err error, i ...interface{}) {
				require.Error(t, err)
				require.ErrorContains(t, err, "error on begin transaction")
			},
			assertTx: func(t *testing.T, repo *Repo, txID string) {
				t.Helper()

				tx, getErr := repo.getTx(txID)
				assert.Nil(t, tx)
				assert.Error(t, getErr)
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			db, mock, err := sqlmock.New()
			require.NoError(t, err)

			tt.setupMock(mock)

			repo := &Repo{
				db:            db,
				insertTimeout: 1000,
				transaction: struct {
					mu sync.Mutex
					tx map[string]*sql.Tx
				}{
					mu: sync.Mutex{},
					tx: make(map[string]*sql.Tx),
				},
			}

			ctx := t.Context()
			err = repo.Begin(ctx, tt.txID)

			tt.wantErr(t, err)

			tt.assertTx(t, repo, tt.txID)

			require.NoError(t, db.Close())
			require.NoError(t, mock.ExpectationsWereMet())
		})
	}
}

//nolint:funlen,dupl // это тест, похожие тест-кейсы
func TestRepo_Exec(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name      string
		txID      string
		repo      func(t *testing.T, db *sql.DB, txID string) *Repo
		request   *storage.Request
		setupMock func(mock sqlmock.Sqlmock)
		wantErr   require.ErrorAssertionFunc
	}{
		{
			name: "positive case: exec succeeds",
			txID: "test-tx-1",
			repo: func(t *testing.T, db *sql.DB, txID string) *Repo {
				t.Helper()

				r := &Repo{
					db:            db,
					insertTimeout: 1000,
					transaction: struct {
						mu sync.Mutex
						tx map[string]*sql.Tx
					}{
						mu: sync.Mutex{},
						tx: make(map[string]*sql.Tx),
					},
				}

				err := r.Begin(t.Context(), txID)
				require.NoError(t, err)

				return r
			},
			request: &storage.Request{
				Val:  "INSERT INTO users (name) VALUES ($1)",
				Args: []any{"John"},
			},
			setupMock: func(mock sqlmock.Sqlmock) {
				mock.ExpectBegin()
				mock.ExpectExec("INSERT INTO users \\(name\\) VALUES \\(\\$1\\)").
					WithArgs("John").
					WillReturnResult(sqlmock.NewResult(1, 1))
			},
			wantErr: require.NoError,
		},
		{
			name: "negative case: transaction not found",
			txID: "non-existent-tx",
			repo: func(t *testing.T, db *sql.DB, txID string) *Repo {
				t.Helper()

				r := &Repo{
					db:            db,
					insertTimeout: 1000,
					transaction: struct {
						mu sync.Mutex
						tx map[string]*sql.Tx
					}{
						mu: sync.Mutex{},
						tx: make(map[string]*sql.Tx),
					},
				}

				return r
			},
			request: &storage.Request{
				Val:  "INSERT INTO users (name) VALUES ($1)",
				Args: []any{"John"},
			},
			setupMock: func(mock sqlmock.Sqlmock) {
				mock.ExpectClose()
			},
			wantErr: func(t require.TestingT, err error, i ...interface{}) {
				require.Error(t, err)
				require.ErrorContains(t, err, "error getting transaction")
			},
		},
		{
			name: "negative case: request value is not a string",
			repo: func(t *testing.T, db *sql.DB, txID string) *Repo {
				t.Helper()

				r := &Repo{
					db:            db,
					insertTimeout: 1000,
					transaction: struct {
						mu sync.Mutex
						tx map[string]*sql.Tx
					}{
						mu: sync.Mutex{},
						tx: make(map[string]*sql.Tx),
					},
				}

				err := r.Begin(t.Context(), txID)
				require.NoError(t, err)

				return r
			},
			txID: "test-tx-2",
			request: &storage.Request{
				Val:  123,
				Args: []any{"John"},
			},
			setupMock: func(mock sqlmock.Sqlmock) {
				mock.ExpectBegin()
			},
			wantErr: func(t require.TestingT, err error, i ...interface{}) {
				require.Error(t, err)
				require.ErrorContains(t, err, "request value is not a string")
			},
		},
		{
			name: "negative case: request arguments are not a slice",
			txID: "test-tx-3",
			repo: func(t *testing.T, db *sql.DB, txID string) *Repo {
				t.Helper()

				r := &Repo{
					db:            db,
					insertTimeout: 1000,
					transaction: struct {
						mu sync.Mutex
						tx map[string]*sql.Tx
					}{
						mu: sync.Mutex{},
						tx: make(map[string]*sql.Tx),
					},
				}

				err := r.Begin(t.Context(), txID)
				require.NoError(t, err)

				return r
			},
			request: &storage.Request{
				Val:  "INSERT INTO users (name) VALUES ($1)",
				Args: "not-a-slice",
			},
			setupMock: func(mock sqlmock.Sqlmock) {
				mock.ExpectBegin()
			},
			wantErr: func(t require.TestingT, err error, i ...interface{}) {
				require.Error(t, err)
				require.ErrorContains(t, err, "request arguments are not a slice of any")
			},
		},
		{
			name: "negative case: exec fails",
			txID: "test-tx-4",
			repo: func(t *testing.T, db *sql.DB, txID string) *Repo {
				t.Helper()

				r := &Repo{
					db:            db,
					insertTimeout: 1000,
					transaction: struct {
						mu sync.Mutex
						tx map[string]*sql.Tx
					}{
						mu: sync.Mutex{},
						tx: make(map[string]*sql.Tx),
					},
				}

				err := r.Begin(t.Context(), txID)
				require.NoError(t, err)

				return r
			},
			request: &storage.Request{
				Val:  "INSERT INTO users (name) VALUES ($1)",
				Args: []any{"John"},
			},
			setupMock: func(mock sqlmock.Sqlmock) {
				mock.ExpectBegin()
				mock.ExpectExec("INSERT INTO users \\(name\\) VALUES \\(\\$1\\)").
					WithArgs("John").
					WillReturnError(errors.New("constraint violation"))
			},
			wantErr: func(t require.TestingT, err error, i ...interface{}) {
				require.Error(t, err)
				require.ErrorContains(t, err, "error executing query")
			},
		},
		{
			name: "positive case: exec with multiple args",
			txID: "test-tx-5",
			repo: func(t *testing.T, db *sql.DB, txID string) *Repo {
				t.Helper()

				r := &Repo{
					db:            db,
					insertTimeout: 1000,
					transaction: struct {
						mu sync.Mutex
						tx map[string]*sql.Tx
					}{
						mu: sync.Mutex{},
						tx: make(map[string]*sql.Tx),
					},
				}

				err := r.Begin(t.Context(), txID)
				require.NoError(t, err)

				return r
			},
			request: &storage.Request{
				Val:  "INSERT INTO users (name, email) VALUES ($1, $2)",
				Args: []any{"John", "john@example.com"},
			},
			setupMock: func(mock sqlmock.Sqlmock) {
				mock.ExpectBegin()
				mock.ExpectExec("INSERT INTO users \\(name, email\\) VALUES \\(\\$1, \\$2\\)").
					WithArgs("John", "john@example.com").
					WillReturnResult(sqlmock.NewResult(1, 1))
			},
			wantErr: require.NoError,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			db, mock, err := sqlmock.New()
			require.NoError(t, err)

			ctx := t.Context()

			tt.setupMock(mock)

			repo := tt.repo(t, db, tt.txID)

			err = repo.Exec(ctx, tt.request, tt.txID)
			tt.wantErr(t, err)

			// mock.ExpectClose()
			require.NoError(t, db.Close())
			require.NoError(t, mock.ExpectationsWereMet())
		})
	}
}

//nolint:funlen,dupl // это тест, похожие тест-кейсы
func TestRepo_Commit(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name      string
		txID      string
		repo      func(t *testing.T, db *sql.DB, txID string) *Repo
		setupMock func(mock sqlmock.Sqlmock)
		assertTx  func(t *testing.T, repo *Repo, txID string)
		wantErr   require.ErrorAssertionFunc
	}{
		{
			name: "positive case: commit succeeds",
			txID: "test-tx-1",
			repo: func(t *testing.T, db *sql.DB, txID string) *Repo {
				t.Helper()

				r := &Repo{
					db:            db,
					insertTimeout: 1000,
					transaction: struct {
						mu sync.Mutex
						tx map[string]*sql.Tx
					}{
						mu: sync.Mutex{},
						tx: make(map[string]*sql.Tx),
					},
				}

				err := r.Begin(t.Context(), txID)
				require.NoError(t, err)

				return r
			},
			assertTx: func(t *testing.T, repo *Repo, txID string) {
				t.Helper()

				_, getErr := repo.getTx(txID)
				require.Error(t, getErr)
				require.ErrorContains(t, getErr, "transaction not found")
			},
			setupMock: func(mock sqlmock.Sqlmock) {
				mock.ExpectBegin()
				mock.ExpectCommit()
			},
			wantErr: require.NoError,
		},
		{
			name: "negative case: transaction not found",
			txID: "non-existent-tx",
			repo: func(t *testing.T, db *sql.DB, txID string) *Repo {
				t.Helper()

				r := &Repo{
					db:            db,
					insertTimeout: 1000,
					transaction: struct {
						mu sync.Mutex
						tx map[string]*sql.Tx
					}{
						mu: sync.Mutex{},
						tx: make(map[string]*sql.Tx),
					},
				}

				return r
			},
			setupMock: func(mock sqlmock.Sqlmock) {
				// не настраиваем транзакцию
			},
			wantErr: func(t require.TestingT, err error, i ...interface{}) {
				require.Error(t, err)
				require.ErrorContains(t, err, "error getting transaction")
			},
			assertTx: func(t *testing.T, repo *Repo, txID string) {
				t.Helper()
			},
		},
		{
			name: "negative case: commit fails",
			txID: "test-tx-2",
			repo: func(t *testing.T, db *sql.DB, txID string) *Repo {
				t.Helper()

				r := &Repo{
					db:            db,
					insertTimeout: 1000,
					transaction: struct {
						mu sync.Mutex
						tx map[string]*sql.Tx
					}{
						mu: sync.Mutex{},
						tx: make(map[string]*sql.Tx),
					},
				}

				err := r.Begin(t.Context(), txID)
				require.NoError(t, err)

				return r
			},
			setupMock: func(mock sqlmock.Sqlmock) {
				mock.ExpectBegin()
				mock.ExpectCommit().WillReturnError(errors.New("connection lost"))
			},
			wantErr: func(t require.TestingT, err error, i ...interface{}) {
				require.Error(t, err)
				require.ErrorContains(t, err, "error committing transaction")
			},
			assertTx: func(t *testing.T, repo *Repo, txID string) {
				t.Helper()
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			db, mock, err := sqlmock.New()
			require.NoError(t, err)

			// Настраиваем моки перед созданием транзакции
			tt.setupMock(mock)

			repo := tt.repo(t, db, tt.txID)

			ctx := t.Context()

			err = repo.Commit(ctx, tt.txID)
			tt.wantErr(t, err)

			// Проверяем, что транзакция удалена после успешного коммита
			tt.assertTx(t, repo, tt.txID)

			mock.ExpectClose()
			require.NoError(t, db.Close())
			require.NoError(t, mock.ExpectationsWereMet())
		})
	}
}

//nolint:funlen,dupl // это тест, похожие тест-кейсы
func TestRepo_Rollback(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name      string
		txID      string
		repo      func(t *testing.T, db *sql.DB, txID string) *Repo
		setupMock func(mock sqlmock.Sqlmock)
		assertTx  func(t *testing.T, repo *Repo, txID string)
		wantErr   require.ErrorAssertionFunc
	}{
		{
			name: "positive case: rollback succeeds",
			txID: "test-tx-1",
			repo: func(t *testing.T, db *sql.DB, txID string) *Repo {
				t.Helper()

				r := &Repo{
					db:            db,
					insertTimeout: 1000,
					transaction: struct {
						mu sync.Mutex
						tx map[string]*sql.Tx
					}{
						mu: sync.Mutex{},
						tx: make(map[string]*sql.Tx),
					},
				}

				err := r.Begin(t.Context(), txID)
				require.NoError(t, err)

				return r
			},
			assertTx: func(t *testing.T, repo *Repo, txID string) {
				t.Helper()

				_, getErr := repo.getTx(txID)
				require.Error(t, getErr)
				require.ErrorContains(t, getErr, "transaction not found")
			},
			setupMock: func(mock sqlmock.Sqlmock) {
				mock.ExpectBegin()
				mock.ExpectRollback()
			},
			wantErr: require.NoError,
		},
		{
			name: "negative case: transaction not found",
			txID: "non-existent-tx",
			repo: func(t *testing.T, db *sql.DB, txID string) *Repo {
				t.Helper()

				r := &Repo{
					db:            db,
					insertTimeout: 1000,
					transaction: struct {
						mu sync.Mutex
						tx map[string]*sql.Tx
					}{
						mu: sync.Mutex{},
						tx: make(map[string]*sql.Tx),
					},
				}

				return r
			},
			setupMock: func(mock sqlmock.Sqlmock) {
				// не настраиваем транзакцию
			},
			wantErr: func(t require.TestingT, err error, i ...interface{}) {
				require.Error(t, err)
				require.ErrorContains(t, err, "error getting transaction")
			},
			assertTx: func(t *testing.T, repo *Repo, txID string) {
				t.Helper()
			},
		},
		{
			name: "negative case: rollback fails",
			txID: "test-tx-2",
			repo: func(t *testing.T, db *sql.DB, txID string) *Repo {
				t.Helper()

				r := &Repo{
					db:            db,
					insertTimeout: 1000,
					transaction: struct {
						mu sync.Mutex
						tx map[string]*sql.Tx
					}{
						mu: sync.Mutex{},
						tx: make(map[string]*sql.Tx),
					},
				}

				err := r.Begin(t.Context(), txID)
				require.NoError(t, err)

				return r
			},
			setupMock: func(mock sqlmock.Sqlmock) {
				mock.ExpectBegin()
				mock.ExpectRollback().WillReturnError(errors.New("connection lost"))
			},
			wantErr: func(t require.TestingT, err error, i ...interface{}) {
				require.Error(t, err)
				require.ErrorContains(t, err, "error rolling back transaction")
			},
			assertTx: func(t *testing.T, repo *Repo, txID string) {
				t.Helper()
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			db, mock, err := sqlmock.New()
			require.NoError(t, err)

			// Настраиваем моки перед созданием транзакции
			tt.setupMock(mock)

			repo := tt.repo(t, db, tt.txID)

			ctx := t.Context()

			err = repo.Rollback(ctx, tt.txID)
			tt.wantErr(t, err)

			mock.ExpectClose()
			require.NoError(t, db.Close())
			require.NoError(t, mock.ExpectationsWereMet())
		})
	}
}

//nolint:funlen,dupl // это тест, похожие тест-кейсы
func TestRepo_FinishTx(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name      string
		txID      string
		repo      func(t *testing.T, db *sql.DB, txID string) *Repo
		setupMock func(mock sqlmock.Sqlmock)
		assertTx  func(t *testing.T, repo *Repo, txID string)
		wantErr   require.ErrorAssertionFunc
	}{
		{
			name: "positive case: finish tx succeeds",
			txID: "test-tx-1",
			repo: func(t *testing.T, db *sql.DB, txID string) *Repo {
				t.Helper()

				r := &Repo{
					db:            db,
					insertTimeout: 1000,
					transaction: struct {
						mu sync.Mutex
						tx map[string]*sql.Tx
					}{
						mu: sync.Mutex{},
						tx: make(map[string]*sql.Tx),
					},
				}

				err := r.Begin(t.Context(), txID)
				require.NoError(t, err)

				return r
			},
			setupMock: func(mock sqlmock.Sqlmock) {
				mock.ExpectBegin()
				mock.ExpectRollback()
			},
			wantErr: require.NoError,
		},
		{
			name: "positive case: transaction not found",
			txID: "non-existent-tx",
			repo: func(t *testing.T, db *sql.DB, txID string) *Repo {
				t.Helper()

				r := &Repo{
					db:            db,
					insertTimeout: 1000,
					transaction: struct {
						mu sync.Mutex
						tx map[string]*sql.Tx
					}{
						mu: sync.Mutex{},
						tx: make(map[string]*sql.Tx),
					},
				}

				return r
			},
			setupMock: func(mock sqlmock.Sqlmock) {
				// не настраиваем транзакцию
			},
			wantErr: require.NoError,
		},
		{
			name: "positive case: rollback fails but ignored",
			txID: "test-tx-2",
			repo: func(t *testing.T, db *sql.DB, txID string) *Repo {
				t.Helper()

				r := &Repo{
					db:            db,
					insertTimeout: 1000,
					transaction: struct {
						mu sync.Mutex
						tx map[string]*sql.Tx
					}{
						mu: sync.Mutex{},
						tx: make(map[string]*sql.Tx),
					},
				}

				err := r.Begin(t.Context(), txID)
				require.NoError(t, err)

				return r
			},
			setupMock: func(mock sqlmock.Sqlmock) {
				mock.ExpectBegin()
				mock.ExpectRollback().WillReturnError(errors.New("transaction already closed"))
			},
			wantErr: require.NoError,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			db, mock, err := sqlmock.New()
			require.NoError(t, err)

			ctx := t.Context()

			// Настраиваем моки перед созданием транзакции
			tt.setupMock(mock)

			repo := tt.repo(t, db, tt.txID)

			err = repo.FinishTx(ctx, tt.txID)
			tt.wantErr(t, err)

			// Проверяем, что транзакция удалена
			_, getErr := repo.getTx(tt.txID)
			require.Error(t, getErr)
			require.ErrorContains(t, getErr, "transaction not found")

			mock.ExpectClose()
			require.NoError(t, db.Close())
			require.NoError(t, mock.ExpectationsWereMet())
		})
	}
}

func TestRepo_Exec_Timeout(t *testing.T) {
	t.Parallel()

	db, mock, err := sqlmock.New()
	require.NoError(t, err)

	repo := &Repo{
		db:            db,
		insertTimeout: 10, // очень короткий таймаут
		transaction: struct {
			mu sync.Mutex
			tx map[string]*sql.Tx
		}{
			mu: sync.Mutex{},
			tx: make(map[string]*sql.Tx),
		},
	}

	ctx := t.Context()
	txID := "test-tx-timeout"

	mock.ExpectBegin()

	err = repo.Begin(ctx, txID)
	require.NoError(t, err)

	// Создаем запрос
	request := &storage.Request{
		Val:  "INSERT INTO users (name) VALUES ($1)",
		Args: []any{"John"},
	}

	// Настраиваем ожидание, что ExecContext будет вызван
	// но из-за таймаута он может не успеть выполниться
	mock.ExpectExec("INSERT INTO users \\(name\\) VALUES \\(\\$1\\)").
		WithArgs("John").
		WillDelayFor(100 * time.Millisecond). // задержка больше таймаута
		WillReturnResult(sqlmock.NewResult(1, 1))

	err = repo.Exec(ctx, request, txID)
	require.Error(t, err)
	require.ErrorContains(t, err, "error executing query")

	require.NoError(t, db.Close())
	require.NoError(t, mock.ExpectationsWereMet())
}
