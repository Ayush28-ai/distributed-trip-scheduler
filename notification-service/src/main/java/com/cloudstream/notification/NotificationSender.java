package com.cloudstream.notification;

/** Delivery channel (SMS, push, email...). Swappable without touching the service. */
public interface NotificationSender {
    void send(Notification notification);
}
