package com.cloudstream.notification;

import jakarta.validation.Valid;
import jakarta.validation.constraints.NotBlank;
import org.springframework.dao.DataIntegrityViolationException;
import org.springframework.data.domain.PageRequest;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestHeader;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

import java.util.List;
import java.util.Map;

@RestController
@RequestMapping("/notifications")
public class NotificationController {

    public record NotificationRequest(@NotBlank String tenantId, String tripId, String message) {
    }

    private final NotificationService service;
    private final NotificationRepository repository;

    public NotificationController(NotificationService service, NotificationRepository repository) {
        this.service = service;
        this.repository = repository;
    }

    /** 201 = sent now, 200 = duplicate suppressed. The scheduler treats both as success. */
    @PostMapping
    public ResponseEntity<Map<String, Object>> send(
            @RequestHeader("Idempotency-Key") String idempotencyKey,
            @RequestHeader(value = "Fencing-Token", required = false) Long fencingToken,
            @RequestHeader(value = "Node-Id", required = false) String nodeId,
            @Valid @RequestBody NotificationRequest body) {
        try {
            NotificationService.SendResult r = service.send(idempotencyKey, body.tenantId(),
                    body.tripId(), body.message(), fencingToken, nodeId);
            if (r.duplicate()) {
                return ResponseEntity.ok(Map.of("duplicate", true));
            }
            return ResponseEntity.status(HttpStatus.CREATED)
                    .body(Map.of("duplicate", false, "id", r.notificationId()));
        } catch (DataIntegrityViolationException race) {
            return ResponseEntity.ok(Map.of("duplicate", true));
        }
    }

    @GetMapping
    public List<Map<String, Object>> list(@RequestParam String tenantId,
                                          @RequestParam(defaultValue = "100") int limit) {
        return repository.findByTenantIdOrderByIdDesc(tenantId, PageRequest.of(0, Math.min(limit, 500)))
                .stream()
                .map(n -> Map.<String, Object>of(
                        "id", n.getId(),
                        "idempotencyKey", n.getIdempotencyKey(),
                        "tripId", n.getTripId() == null ? "" : n.getTripId(),
                        "fencingToken", n.getFencingToken() == null ? -1 : n.getFencingToken(),
                        "nodeId", n.getNodeId() == null ? "" : n.getNodeId(),
                        "sentAt", n.getSentAt().toString()))
                .toList();
    }

    /** total == distinctKeys proves no alert was sent twice. */
    @GetMapping("/stats")
    public Map<String, Object> stats(@RequestParam String tenantId) {
        return Map.of("tenantId", tenantId,
                "total", repository.countByTenantId(tenantId),
                "distinctKeys", repository.countDistinctKeysByTenantId(tenantId));
    }
}
