package com.cipherlab.campaign_processor.entity;

import jakarta.persistence.*;
import lombok.*;
import java.time.Instant;

@Entity
@Table(name = "campaign_stats", uniqueConstraints = {
    @UniqueConstraint(columnNames = {"campaignId", "hourBucket"})
})
@Data @NoArgsConstructor @AllArgsConstructor
public class CampaignStats {
    @Id
    @GeneratedValue(strategy = GenerationType.IDENTITY)
    private Long id;

    private Long campaignId;
    private Instant hourBucket; // Truncated to the start of the hour
    
    private long clickCount;
    private long impressionCount;
}
