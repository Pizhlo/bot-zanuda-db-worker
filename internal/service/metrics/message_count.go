package metrics

import "github.com/sirupsen/logrus"

// AddProcessingMessages добавляет количество сообщений в процессе обработки.
func (s *Service) AddProcessingMessages(count int) {
	s.processingMessages.Add(float64(count))

	logrus.WithFields(logrus.Fields{
		"count": count,
	}).Debug("metrics: add processing messages")
}

// AddFailedMessages добавляет количество сообщений в статусе failed.
func (s *Service) AddFailedMessages(count int) {
	s.failedMessages.Add(float64(count))

	logrus.WithFields(logrus.Fields{
		"count": count,
	}).Debug("metrics: add failed messages")
}

// AddValidatedMessages добавляет количество сообщений в статусе validated.
func (s *Service) AddValidatedMessages(count int) {
	s.validatedMessages.Add(float64(count))

	logrus.WithFields(logrus.Fields{
		"count": count,
	}).Debug("metrics: add validated messages")
}

// AddProcessedMessages добавляет количество обработанных сообщений.
func (s *Service) AddProcessedMessages(count int) {
	s.processedMessages.Add(float64(count))

	logrus.WithFields(logrus.Fields{
		"count": count,
	}).Debug("metrics: add processed messages")
}

// AddTotalMessages добавляет общее количество сообщений.
func (s *Service) AddTotalMessages(count int) {
	s.totalMessages.Add(float64(count))

	logrus.WithFields(logrus.Fields{
		"count": count,
	}).Debug("metrics: add total messages")
}

// DecrementProcessingMessagesBy уменьшает количество сообщений в процессе обработки на count.
func (s *Service) DecrementProcessingMessagesBy(count int) {
	s.processingMessages.Sub(float64(count))

	logrus.WithFields(logrus.Fields{
		"count": count,
	}).Debug("metrics: decrement processing messages by")
}

// DecrementFailedMessagesBy уменьшает количество сообщений в статусе failed на count.
func (s *Service) DecrementFailedMessagesBy(count int) {
	s.failedMessages.Sub(float64(count))

	logrus.WithFields(logrus.Fields{
		"count": count,
	}).Debug("metrics: decrement failed messages by")
}
