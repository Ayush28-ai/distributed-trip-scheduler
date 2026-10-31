package com.cloudstream.notification;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;

/** Fake channel: logs ids only. No employee name, phone or address is ever logged. */
@Component
public class LogNotificationSender implements NotificationSender {

    private static final Logger log = LoggerFactory.getLogger(LogNotificationSender.class);

    @Override
    public void send(Notification n) {
        log.info("notification_sent tenant={} trip={} token={} node={}",
                n.getTenantId(), n.getTripId(), n.getFencingToken(), n.getNodeId());
    }
}
