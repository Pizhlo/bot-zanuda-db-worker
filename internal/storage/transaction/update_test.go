package transaction

import (
	"database/sql"
	"db-worker/internal/storage"
	"testing"

	"github.com/DATA-DOG/go-sqlmock"
	"github.com/stretchr/testify/require"
)

func TestUpdateStatusMany(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name      string
		repo      func(t *testing.T, db *sql.DB) *Repo
		setupMock func(mock sqlmock.Sqlmock)
		ids       []string
		status    string
		errMsg    string
		wantErr   require.ErrorAssertionFunc
	}{
		{
			name:   "positive case",
			ids:    []string{"1", "2", "3"},
			status: string(storage.TxStatusInProgress),
			setupMock: func(mock sqlmock.Sqlmock) {
				t.Helper()

				mock.ExpectBegin()
				mock.ExpectExec("UPDATE transactions.transactions SET status = \\$1, error = \\$2 WHERE id IN \\(\\$3, \\$4, \\$5\\)").
					WithArgs(string(storage.TxStatusInProgress), "", "1", "2", "3").
					WillReturnResult(sqlmock.NewResult(1, 3))
				mock.ExpectCommit()
			},
			errMsg:  "",
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

			err = repo.UpdateStatusMany(t.Context(), tt.ids, tt.status, tt.errMsg)
			tt.wantErr(t, err)

			require.NoError(t, mock.ExpectationsWereMet())

			mock.ExpectClose()
			require.NoError(t, db.Close())
		})
	}
}
