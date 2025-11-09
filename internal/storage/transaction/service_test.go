package transaction

import (
	"context"
	"database/sql"
	"db-worker/internal/config"
	"errors"
	"reflect"
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
