package postgres

import (
	"context"
	"database/sql"
	"db-worker/internal/config"
	"db-worker/internal/config/operation"
	"errors"
	"reflect"
	"sync"
	"testing"
	"time"

	"github.com/DATA-DOG/go-sqlmock"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

//nolint:funlen // это тест
func TestNew(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name    string
		opts    []RepoOption
		want    *Repo
		wantErr require.ErrorAssertionFunc
	}{
		{
			name: "positive case",
			opts: []RepoOption{
				WithAddr("postgres://user:password@localhost:5432/dbname?sslmode=disable"),
				WithName("test-repo"),
				WithInsertTimeout(1),
				WithReadTimeout(2),
				WithCfg(&config.Postgres{
					Host:          "localhost",
					Port:          5432,
					User:          "user",
					Password:      "password",
					DBName:        "dbname",
					InsertTimeout: 1,
					ReadTimeout:   2,
				}),
			},
			want: &Repo{
				addr:          "postgres://user:password@localhost:5432/dbname?sslmode=disable",
				name:          "test-repo",
				insertTimeout: 1,
				readTimeout:   2,
			},
			wantErr: require.NoError,
		},
		{
			name: "negative case: addr is empty",
			opts: []RepoOption{
				WithName("test-repo"),
				WithInsertTimeout(1),
				WithReadTimeout(2),
				WithCfg(&config.Postgres{
					Host:          "localhost",
					Port:          5432,
					User:          "user",
					Password:      "password",
					DBName:        "dbname",
					InsertTimeout: 1,
					ReadTimeout:   2,
				}),
			},
			want: nil,
			wantErr: func(t require.TestingT, err error, i ...interface{}) {
				require.Error(t, err)
				require.ErrorContains(t, err, "addr is required")
			},
		},
		{
			name: "negative case: insert timeout is 0",
			opts: []RepoOption{
				WithAddr("postgres://user:password@localhost:5432/dbname?sslmode=disable"),
				WithName("test-repo"),
				WithReadTimeout(2),
				WithCfg(&config.Postgres{
					Host:          "localhost",
					Port:          5432,
					User:          "user",
					Password:      "password",
					DBName:        "dbname",
					InsertTimeout: 1,
					ReadTimeout:   2,
				}),
			},
			want: nil,
			wantErr: func(t require.TestingT, err error, i ...interface{}) {
				require.Error(t, err)
				require.ErrorContains(t, err, "insert timeout is required")
			},
		},
		{
			name: "negative case: read timeout is 0",
			opts: []RepoOption{
				WithAddr("postgres://user:password@localhost:5432/dbname?sslmode=disable"),
				WithName("test-repo"),
				WithInsertTimeout(1),
				WithCfg(&config.Postgres{
					Host:          "localhost",
					Port:          5432,
					User:          "user",
					Password:      "password",
					DBName:        "dbname",
					InsertTimeout: 1,
					ReadTimeout:   2,
				}),
			},
			want: nil,
			wantErr: func(t require.TestingT, err error, i ...interface{}) {
				require.Error(t, err)
				require.ErrorContains(t, err, "read timeout is required")
			},
		},
		{
			name: "negative case: name is empty",
			opts: []RepoOption{
				WithAddr("postgres://user:password@localhost:5432/dbname?sslmode=disable"),
				WithInsertTimeout(1),
				WithReadTimeout(2),
				WithCfg(&config.Postgres{
					Host:          "localhost",
					Port:          5432,
					User:          "user",
					Password:      "password",
					DBName:        "dbname",
					InsertTimeout: 1,
					ReadTimeout:   2,
				}),
			},
			want: nil,
			wantErr: func(t require.TestingT, err error, i ...interface{}) {
				require.Error(t, err)
				require.ErrorContains(t, err, "name is required")
			},
		},
		{
			name: "negative case: config is nil",
			opts: []RepoOption{
				WithAddr("postgres://user:password@localhost:5432/dbname?sslmode=disable"),
				WithName("test-repo"),
				WithInsertTimeout(1),
				WithReadTimeout(2),
			},
			want: nil,
			wantErr: func(t require.TestingT, err error, i ...interface{}) {
				require.Error(t, err)
				require.ErrorContains(t, err, "config is required")
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			got, err := New(t.Context(), tt.opts...)
			tt.wantErr(t, err)

			if tt.want != nil {
				require.NotNil(t, got)

				assert.Equal(t, tt.want.addr, got.addr)
				assert.Equal(t, tt.want.name, got.name)
				assert.Equal(t, tt.want.insertTimeout, got.insertTimeout)
				assert.Equal(t, tt.want.readTimeout, got.readTimeout)

				assert.NotNil(t, got.db)
				assert.Equal(t, reflect.TypeOf(&sql.DB{}), reflect.TypeOf(got.db))

				// Закрываем соединение с базой данных для предотвращения утечек ресурсов
				if got.db != nil {
					_ = got.db.Close()
				}
			}
		})
	}
}

//nolint:dupl // похожие тест-кейсы
func TestRepo_Name(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name     string
		repoName string
		want     string
	}{
		{
			name:     "positive case: name is set",
			repoName: "test-repo",
			want:     "test-repo",
		},
		{
			name:     "empty name",
			repoName: "",
			want:     "",
		},
		{
			name:     "name with special characters",
			repoName: "test-repo-123",
			want:     "test-repo-123",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			repo := &Repo{
				name: tt.repoName,
			}

			got := repo.Name()
			assert.Equal(t, tt.want, got)
		})
	}
}

func TestRepo_Type(t *testing.T) {
	t.Parallel()

	repo := &Repo{}

	got := repo.Type()
	assert.Equal(t, operation.StorageTypePostgres, got)
}

//nolint:dupl // похожие тест-кейсы
func TestRepo_Table(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name      string
		tableName string
		want      string
	}{
		{
			name:      "positive case: table name is set",
			tableName: "users",
			want:      "users",
		},
		{
			name:      "empty table name",
			tableName: "",
			want:      "",
		},
		{
			name:      "table name with schema",
			tableName: "public.users",
			want:      "public.users",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			repo := &Repo{
				table: tt.tableName,
			}

			got := repo.Table()
			assert.Equal(t, tt.want, got)
		})
	}
}

//nolint:dupl // похожие тест-кейсы
func TestRepo_Host(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name string
		cfg  *config.Postgres
		want string
	}{
		{
			name: "positive case: host is set",
			cfg: &config.Postgres{
				Host: "localhost",
			},
			want: "localhost",
		},
		{
			name: "empty host",
			cfg: &config.Postgres{
				Host: "",
			},
			want: "",
		},
		{
			name: "host with domain",
			cfg: &config.Postgres{
				Host: "db.example.com",
			},
			want: "db.example.com",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			repo := &Repo{
				cfg: tt.cfg,
			}

			got := repo.Host()
			assert.Equal(t, tt.want, got)
		})
	}
}

//nolint:dupl // похожие тест-кейсы
func TestRepo_User(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name string
		cfg  *config.Postgres
		want string
	}{
		{
			name: "positive case: user is set",
			cfg: &config.Postgres{
				User: "postgres",
			},
			want: "postgres",
		},
		{
			name: "empty user",
			cfg: &config.Postgres{
				User: "",
			},
			want: "",
		},
		{
			name: "user with special characters",
			cfg: &config.Postgres{
				User: "test_user",
			},
			want: "test_user",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			repo := &Repo{
				cfg: tt.cfg,
			}

			got := repo.User()
			assert.Equal(t, tt.want, got)
		})
	}
}

//nolint:dupl // похожие тест-кейсы
func TestRepo_Password(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name string
		cfg  *config.Postgres
		want string
	}{
		{
			name: "positive case: password is set",
			cfg: &config.Postgres{
				Password: "secret123",
			},
			want: "secret123",
		},
		{
			name: "empty password",
			cfg: &config.Postgres{
				Password: "",
			},
			want: "",
		},
		{
			name: "password with special characters",
			cfg: &config.Postgres{
				Password: "p@ssw0rd!",
			},
			want: "p@ssw0rd!",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			repo := &Repo{
				cfg: tt.cfg,
			}

			got := repo.Password()
			assert.Equal(t, tt.want, got)
		})
	}
}

//nolint:dupl // похожие тест-кейсы
func TestRepo_DBName(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name string
		cfg  *config.Postgres
		want string
	}{
		{
			name: "positive case: db name is set",
			cfg: &config.Postgres{
				DBName: "mydb",
			},
			want: "mydb",
		},
		{
			name: "empty db name",
			cfg: &config.Postgres{
				DBName: "",
			},
			want: "",
		},
		{
			name: "db name with underscore",
			cfg: &config.Postgres{
				DBName: "my_database",
			},
			want: "my_database",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			repo := &Repo{
				cfg: tt.cfg,
			}

			got := repo.DBName()
			assert.Equal(t, tt.want, got)
		})
	}
}

func TestRepo_Queue(t *testing.T) {
	t.Parallel()

	repo := &Repo{}

	got := repo.Queue()
	assert.Equal(t, "", got)
}

func TestRepo_RoutingKey(t *testing.T) {
	t.Parallel()

	repo := &Repo{}

	got := repo.RoutingKey()
	assert.Equal(t, "", got)
}

//nolint:dupl // похожие тест-кейсы
func TestRepo_InsertTimeout(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name          string
		insertTimeout int
		want          int
	}{
		{
			name:          "positive case: timeout is set",
			insertTimeout: 10,
			want:          10,
		},
		{
			name:          "zero timeout",
			insertTimeout: 0,
			want:          0,
		},
		{
			name:          "large timeout",
			insertTimeout: 300,
			want:          300,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			repo := &Repo{
				insertTimeout: tt.insertTimeout,
			}

			got := repo.InsertTimeout()
			assert.Equal(t, tt.want, got)
		})
	}
}

//nolint:dupl // похожие тест-кейсы
func TestRepo_ReadTimeout(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name        string
		readTimeout int
		want        int
	}{
		{
			name:        "positive case: timeout is set",
			readTimeout: 5,
			want:        5,
		},
		{
			name:        "zero timeout",
			readTimeout: 0,
			want:        0,
		},
		{
			name:        "large timeout",
			readTimeout: 600,
			want:        600,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			repo := &Repo{
				readTimeout: tt.readTimeout,
			}

			got := repo.ReadTimeout()
			assert.Equal(t, tt.want, got)
		})
	}
}

//nolint:dupl // похожие тест-кейсы
func TestRepo_Port(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name string
		cfg  *config.Postgres
		want int
	}{
		{
			name: "positive case: port is set",
			cfg: &config.Postgres{
				Port: 5432,
			},
			want: 5432,
		},
		{
			name: "zero port",
			cfg: &config.Postgres{
				Port: 0,
			},
			want: 0,
		},
		{
			name: "custom port",
			cfg: &config.Postgres{
				Port: 5433,
			},
			want: 5433,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			repo := &Repo{
				cfg: tt.cfg,
			}

			got := repo.Port()
			assert.Equal(t, tt.want, got)
		})
	}
}

//nolint:funlen,dupl // это тест, похожие тест-кейсы
func TestRepo_getTx(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name      string
		setupRepo func() *Repo
		txID      string
		wantErr   require.ErrorAssertionFunc
	}{
		{
			name: "positive case: transaction exists",
			setupRepo: func() *Repo {
				repo := &Repo{}
				repo.transaction = struct {
					mu sync.Mutex
					tx map[string]*sql.Tx
				}{
					mu: sync.Mutex{},
					tx: make(map[string]*sql.Tx),
				}
				// Симулируем транзакцию (не можем создать реальную без БД)
				tx := &sql.Tx{}
				repo.transaction.tx["test-tx-1"] = tx

				return repo
			},
			txID: "test-tx-1",
			wantErr: func(t require.TestingT, err error, i ...interface{}) {
				require.NoError(t, err)
			},
		},
		{
			name: "negative case: transaction not found",
			setupRepo: func() *Repo {
				repo := &Repo{}
				repo.transaction = struct {
					mu sync.Mutex
					tx map[string]*sql.Tx
				}{
					mu: sync.Mutex{},
					tx: make(map[string]*sql.Tx),
				}

				return repo
			},
			txID: "non-existent-tx",
			wantErr: func(t require.TestingT, err error, i ...interface{}) {
				require.Error(t, err)
				require.ErrorContains(t, err, "transaction not found")
			},
		},
		{
			name: "negative case: empty transaction map",
			setupRepo: func() *Repo {
				repo := &Repo{}
				repo.transaction = struct {
					mu sync.Mutex
					tx map[string]*sql.Tx
				}{
					mu: sync.Mutex{},
					tx: make(map[string]*sql.Tx),
				}

				return repo
			},
			txID: "test-tx-1",
			wantErr: func(t require.TestingT, err error, i ...interface{}) {
				require.Error(t, err)
				require.ErrorContains(t, err, "transaction not found")
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			repo := tt.setupRepo()
			got, err := repo.getTx(tt.txID)
			tt.wantErr(t, err)

			if err == nil {
				assert.NotNil(t, got)
			} else {
				assert.Nil(t, got)
			}
		})
	}
}

//nolint:funlen // это тест
func TestRepo_Run(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name      string
		setupMock func(mock sqlmock.Sqlmock)
		wantErr   require.ErrorAssertionFunc
	}{
		{
			name: "positive case: ping succeeds",
			setupMock: func(mock sqlmock.Sqlmock) {
				mock.ExpectPing()
			},
			wantErr: require.NoError,
		},
		{
			name: "negative case: ping fails",
			setupMock: func(mock sqlmock.Sqlmock) {
				mock.ExpectPing().WillReturnError(errors.New("connection refused"))
			},
			wantErr: func(t require.TestingT, err error, i ...interface{}) {
				require.Error(t, err)
				require.ErrorContains(t, err, "error pinging db")
			},
		},
		{
			name: "negative case: ping timeout",
			setupMock: func(mock sqlmock.Sqlmock) {
				mock.ExpectPing().WillReturnError(errors.New("context deadline exceeded"))
			},
			wantErr: func(t require.TestingT, err error, i ...interface{}) {
				require.Error(t, err)
				require.ErrorContains(t, err, "error pinging db")
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			db, mock, err := sqlmock.New(sqlmock.MonitorPingsOption(true))
			require.NoError(t, err)

			tt.setupMock(mock)

			repo := &Repo{
				db:   db,
				addr: "postgres://user:password@localhost:5432/dbname",
				name: "test-repo",
			}

			ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			defer cancel()

			err = repo.Run(ctx)
			tt.wantErr(t, err)

			mock.ExpectClose()
			require.NoError(t, db.Close())
			require.NoError(t, mock.ExpectationsWereMet())
		})
	}
}

func TestRepo_Stop(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name      string
		setupMock func(mock sqlmock.Sqlmock)
		wantErr   require.ErrorAssertionFunc
	}{
		{
			name: "positive case: close succeeds",
			setupMock: func(mock sqlmock.Sqlmock) {
				mock.ExpectClose()
			},
			wantErr: require.NoError,
		},
		{
			name: "negative case: close fails",
			setupMock: func(mock sqlmock.Sqlmock) {
				mock.ExpectClose().WillReturnError(errors.New("connection already closed"))
			},
			wantErr: func(t require.TestingT, err error, i ...interface{}) {
				require.Error(t, err)
				require.ErrorContains(t, err, "connection already closed")
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
				db: db,
			}

			ctx := context.Background()
			err = repo.Stop(ctx)
			tt.wantErr(t, err)

			require.NoError(t, mock.ExpectationsWereMet())
		})
	}
}
