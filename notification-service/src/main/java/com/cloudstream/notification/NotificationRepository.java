package com.cloudstream.notification;

import org.springframework.data.domain.Pageable;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.data.jpa.repository.Query;

import java.util.List;

public interface NotificationRepository extends JpaRepository<Notification, Long> {

    boolean existsByIdempotencyKey(String idempotencyKey);

    List<Notification> findByTenantIdOrderByIdDesc(String tenantId, Pageable pageable);

    long countByTenantId(String tenantId);

    @Query("select count(distinct n.idempotencyKey) from Notification n where n.tenantId = ?1")
    long countDistinctKeysByTenantId(String tenantId);
}
