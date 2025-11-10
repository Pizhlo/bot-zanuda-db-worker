package metrics

import "github.com/sirupsen/logrus"

// AddTotalTransactions добавляет общее количество транзакций.
func (s *Service) AddTotalTransactions(count int) {
	s.totalTransactions.Add(float64(count))

	logrus.WithFields(logrus.Fields{
		"count": count,
	}).Debug("metrics: add total transactions")
}

// AddInProgressTransactions добавляет количество транзакций в статусе in progress.
func (s *Service) AddInProgressTransactions(count int) {
	s.inProgressTransactions.Add(float64(count))

	logrus.WithFields(logrus.Fields{
		"count": count,
	}).Debug("metrics: add in progress transactions")
}

// AddFailedTransactions добавляет количество транзакций в статусе failed.
func (s *Service) AddFailedTransactions(count int) {
	s.failedTransactions.Add(float64(count))

	logrus.WithFields(logrus.Fields{
		"count": count,
	}).Debug("metrics: add failed transactions")
}

// AddCanceledTransactions добавляет количество транзакций в статусе canceled.
func (s *Service) AddCanceledTransactions(count int) {
	s.canceledTransactions.Add(float64(count))

	logrus.WithFields(logrus.Fields{
		"count": count,
	}).Debug("metrics: add canceled transactions")
}

// AddSuccessTransactions добавляет количество транзакций в статусе success.
func (s *Service) AddSuccessTransactions(count int) {
	s.successTransactions.Add(float64(count))

	logrus.WithFields(logrus.Fields{
		"count": count,
	}).Debug("metrics: add success transactions")
}

// DecrementTotalTransactions уменьшает общее количество транзакций на count.
func (s *Service) DecrementTotalTransactions(count int) {
	s.totalTransactions.Sub(float64(count))

	logrus.WithFields(logrus.Fields{
		"count": count,
	}).Debug("metrics: decrement total transactions")
}

// DecrementInProgressTransactions уменьшает количество транзакций в статусе in progress на count.
func (s *Service) DecrementInProgressTransactions(count int) {
	s.inProgressTransactions.Sub(float64(count))

	logrus.WithFields(logrus.Fields{
		"count": count,
	}).Debug("metrics: decrement in progress transactions")
}

// DecrementFailedTransactions уменьшает количество транзакций в статусе failed на count.
func (s *Service) DecrementFailedTransactions(count int) {
	s.failedTransactions.Sub(float64(count))

	logrus.WithFields(logrus.Fields{
		"count": count,
	}).Debug("metrics: decrement failed transactions")
}

// DecrementCanceledTransactions уменьшает количество транзакций в статусе canceled на count.
func (s *Service) DecrementCanceledTransactions(count int) {
	s.canceledTransactions.Sub(float64(count))

	logrus.WithFields(logrus.Fields{
		"count": count,
	}).Debug("metrics: decrement canceled transactions")
}

// DecrementSuccessTransactions уменьшает количество транзакций в статусе success на count.
func (s *Service) DecrementSuccessTransactions(count int) {
	s.successTransactions.Sub(float64(count))

	logrus.WithFields(logrus.Fields{
		"count": count,
	}).Debug("metrics: decrement success transactions")
}
