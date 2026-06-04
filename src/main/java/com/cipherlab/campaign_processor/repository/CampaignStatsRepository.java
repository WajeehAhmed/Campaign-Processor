package com.cipherlab.campaign_processor.repository;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.data.jpa.repository.Modifying;
import org.springframework.data.jpa.repository.Query;
import org.springframework.transaction.annotation.Transactional;

import com.cipherlab.campaign_processor.entity.CampaignStats;

import java.time.Instant;
import java.util.List;

public interface CampaignStatsRepository extends JpaRepository<CampaignStats, Long> {
    
    @Modifying
    @Transactional
    @Query(value = """
        INSERT INTO campaign_stats (campaign_id, hour_bucket, click_count, impression_count)
        VALUES (:campaignId, :hour, :clicks, :impressions)
        ON CONFLICT (campaign_id, hour_bucket)
        DO UPDATE SET 
            click_count = campaign_stats.click_count + EXCLUDED.click_count,
            impression_count = campaign_stats.impression_count + EXCLUDED.impression_count
        """, nativeQuery = true)
    void upsertStats(Long campaignId, Instant hour, long clicks, long impressions);

    List<CampaignStats> findByCampaignIdAndHourBucketBetweenOrderByHourBucketAsc(
        Long campaignId, Instant start, Instant end);
}