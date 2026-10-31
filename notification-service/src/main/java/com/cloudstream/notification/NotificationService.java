package com.cloudstream.notification;

import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

/**
 * Exactly-once effect: the first request for an idempotency key inserts a row and sends;
 * every later request with the same key (a retry, or a node that took over) is reported
 * as a duplicate and sends nothing.
 *
 * The insert and the send are in one transaction: if sending throws, the row is rolled
 * back and the scheduler's retry can send again. Two concurrent first requests race on
 * the UNIQUE constraint; the loser gets DataIntegrityViolationException, which the
 * controller maps to "duplicate".
 */
@Service
public class NotificationService {

    public record SendResult(boolean duplicate, Long notificationId) {
    }

    private final NotificationRepository repository;
    private final NotificationSender sender;

    public NotificationService(NotificationRepository repository, NotificationSender sender) {
        this.repository = repository;
        this.sender = sender;
    }

    @Transactional
    public SendResult send(String idempotencyKey, String tenantId, String tripId, String message,
                           Long fencingToken, String nodeId) {
        if (repository.existsByIdempotencyKey(idempotencyKey)) {
            return new SendResult(true, null);
        }
        Notification saved = repository.saveAndFlush(
                new Notification(idempotencyKey, tenantId, tripId, message, fencingToken, nodeId));
        sender.send(saved);
        return new SendResult(false, saved.getId());
    }
}
