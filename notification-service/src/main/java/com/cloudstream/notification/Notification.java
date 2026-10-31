package com.cloudstream.notification;

import jakarta.persistence.Column;
import jakarta.persistence.Entity;
import jakarta.persistence.GeneratedValue;
import jakarta.persistence.GenerationType;
import jakarta.persistence.Id;
import jakarta.persistence.Table;

import java.time.OffsetDateTime;

/** One sent notification. idempotency_key is UNIQUE in the database (schema.sql). */
@Entity
@Table(name = "notifications")
public class Notification {

    @Id
    @GeneratedValue(strategy = GenerationType.IDENTITY)
    private Long id;

    @Column(name = "idempotency_key", nullable = false, unique = true)
    private String idempotencyKey;

    @Column(name = "tenant_id", nullable = false)
    private String tenantId;

    @Column(name = "trip_id")
    private String tripId;

    private String message;

    @Column(name = "fencing_token")
    private Long fencingToken;

    @Column(name = "node_id")
    private String nodeId;

    @Column(name = "sent_at", nullable = false)
    private OffsetDateTime sentAt;

    protected Notification() {
    }

    public Notification(String idempotencyKey, String tenantId, String tripId, String message,
                        Long fencingToken, String nodeId) {
        this.idempotencyKey = idempotencyKey;
        this.tenantId = tenantId;
        this.tripId = tripId;
        this.message = message;
        this.fencingToken = fencingToken;
        this.nodeId = nodeId;
        this.sentAt = OffsetDateTime.now();
    }

    public Long getId() { return id; }
    public String getIdempotencyKey() { return idempotencyKey; }
    public String getTenantId() { return tenantId; }
    public String getTripId() { return tripId; }
    public String getMessage() { return message; }
    public Long getFencingToken() { return fencingToken; }
    public String getNodeId() { return nodeId; }
    public OffsetDateTime getSentAt() { return sentAt; }
}
