package operation

// addFailedMessages добавляет количество сообщений в статусе failed и уменьшает количество сообщений в процессе обработки.
func (s *Service) addFailedMessages(count int) {
	s.metricsService.AddFailedMessages(count)
	s.metricsService.DecrementProcessingMessagesBy(count)
}

// addValidatedMessages добавляет количество сообщений в статусе validated и уменьшает количество сообщений в процессе обработки.
func (s *Service) addValidatedMessages(count int) {
	s.metricsService.AddValidatedMessages(count)
	s.metricsService.DecrementProcessingMessagesBy(count)
}

// addProcessingMessages добавляет количество сообщений в процессе обработки.
func (s *Service) addProcessingMessages(count int) {
	s.metricsService.AddProcessingMessages(count)
}

// fromFailedToValidated добавляет количество сообщений в статусе validated и уменьшает количество сообщений в статусе failed.
func (s *Service) fromFailedToValidated(count int) {
	s.metricsService.AddValidatedMessages(count)
	s.metricsService.DecrementFailedMessagesBy(count)
}

// addTotalMessages добавляет общее количество сообщений.
func (s *Service) addTotalMessages(count int) {
	s.metricsService.AddTotalMessages(count)
}

// addProcessedMessages добавляет количество обработанных сообщений.
func (s *Service) addProcessedMessages(count int) {
	s.metricsService.AddProcessedMessages(count)
}
