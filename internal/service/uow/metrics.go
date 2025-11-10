package uow

// addTotalTransactions добавляет общее количество транзакций.
func (s *Service) addTotalTransactions(count int) {
	s.metricsService.AddTotalTransactions(count)
}

// addInProgressTransactions добавляет количество транзакций в статусе in progress.
func (s *Service) addInProgressTransactions(count int) {
	s.metricsService.AddInProgressTransactions(count)
}

// addFailedTransactions добавляет количество транзакций в статусе failed.
func (s *Service) addFailedTransactions(count int) {
	s.metricsService.AddFailedTransactions(count)
	s.metricsService.DecrementInProgressTransactions(count)
}

// addCanceledTransactions добавляет количество транзакций в статусе canceled.
func (s *Service) addCanceledTransactions(count int) {
	s.metricsService.AddCanceledTransactions(count)
	s.metricsService.DecrementInProgressTransactions(count)
}

// addSuccessTransactions добавляет количество транзакций в статусе success.
func (s *Service) addSuccessTransactions(count int) {
	s.metricsService.AddSuccessTransactions(count)
	s.metricsService.DecrementInProgressTransactions(count)
}
