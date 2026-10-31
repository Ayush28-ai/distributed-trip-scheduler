package com.cloudstream.notification;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

class NotificationServiceTest {

    private final NotificationRepository repository = mock(NotificationRepository.class);
    private final NotificationSender sender = mock(NotificationSender.class);
    private final NotificationService service = new NotificationService(repository, sender);

    @Test
    void firstRequestSendsOnce() {
        when(repository.existsByIdempotencyKey("acme:1:t")).thenReturn(false);
        when(repository.saveAndFlush(any(Notification.class))).thenAnswer(inv -> inv.getArgument(0));

        NotificationService.SendResult r = service.send("acme:1:t", "acme", "T1", "hi", 7L, "node1");

        assertFalse(r.duplicate());
        verify(sender, times(1)).send(any(Notification.class));
    }

    @Test
    void repeatedKeyIsSuppressed() {
        when(repository.existsByIdempotencyKey("acme:1:t")).thenReturn(true);

        NotificationService.SendResult r = service.send("acme:1:t", "acme", "T1", "hi", 8L, "node2");

        assertTrue(r.duplicate());
        verify(repository, never()).saveAndFlush(any(Notification.class));
        verify(sender, never()).send(any(Notification.class));
    }
}
