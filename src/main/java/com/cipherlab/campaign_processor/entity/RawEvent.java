package com.cipherlab.campaign_processor.entity;

import jakarta.persistence.*;
import lombok.*;
import java.time.Instant;
import java.util.UUID;

@Entity
@Table(name = "raw_events")
@Data @NoArgsConstructor @AllArgsConstructor @Builder
public class RawEvent {
    @Id
    @GeneratedValue(strategy = GenerationType.UUID)
    private UUID id;
    @Column(name = "campaign_id", nullable = false)
    private Long campaignId;
    @Column(name = "event_type", nullable = false)
    private String eventType; // "CLICK" or "IMPRESSION"
    
    @Column(name = "timestamp", nullable = false)
    private Instant timestamp;

    @PrePersist
    protected void onCreate() {
        if (this.timestamp == null) this.timestamp = Instant.now();
    }
}