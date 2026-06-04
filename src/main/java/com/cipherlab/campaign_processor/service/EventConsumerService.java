package com.cipherlab.campaign_processor.service;

import lombok.extern.slf4j.Slf4j;

import java.time.Duration;
import java.time.Instant;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.time.ZoneOffset;
import java.time.temporal.ChronoUnit;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.support.KafkaHeaders;
import org.springframework.messaging.handler.annotation.Header;
import org.springframework.stereotype.Service;
import org.springframework.data.redis.core.StringRedisTemplate;

import com.cipherlab.campaign_processor.dto.CampaignEventMessage;
import com.cipherlab.campaign_processor.entity.RawEvent;
import com.cipherlab.campaign_processor.repository.CampaignStatsRepository;
import com.cipherlab.campaign_processor.repository.RawEventRepository;

@Service
@Slf4j
public class EventConsumerService {
    @Autowired
    private RawEventRepository rawEventRepository;
    @Autowired
    private CampaignStatsRepository campaignStatsRepository;
    @Autowired
    private StringRedisTemplate redisTemplate;

    @KafkaListener(topics = "campaign-events-raw", groupId = "campaign-processor-group")
    public void listen(
            CampaignEventMessage message,
            @Header(KafkaHeaders.RECEIVED_PARTITION) int partition,
            @Header(KafkaHeaders.RECEIVED_KEY) String key) throws Exception{

        // Construct a unique caching key using the individual event's UUID
        String deduplicationKey = "event:processed:" + message.getEventId();

        // Atomically set the key with a 24-hour expiration window if it doesn't exist
        Boolean isNewEvent = redisTemplate.opsForValue().setIfAbsent(deduplicationKey, "true", Duration.ofHours(24));

        // Guard Rail: If false, Kafka redelivered a message we already fully committed. Drop it!
        if (Boolean.FALSE.equals(isNewEvent)) {
            log.warn("[IDEMPOTENCY GUARD] Duplicate event detected! EventID: {} on Partition: {}. Dropping instantly.", 
                    message.getEventId(), partition);
            return; 
        }

        try {
            log.info("[PARTITION {}] Processing unique event: {}", partition, message.getEventId());
            LocalDateTime eventTime = LocalDateTime.ofInstant(message.getTimestamp(), ZoneId.of("UTC"));
            // Truncate timestamp to the nearest hour to identify the correct stats bucket
            LocalDateTime hourBucket = eventTime.truncatedTo(ChronoUnit.HOURS);


            RawEvent rawEvent = RawEvent.builder()
                    .campaignId(message.getCampaignId())
                    .eventType(message.getEventType())
                    .build();
            // SIMULATE PRODUCTION BOTTLENECK: Freeze the thread for 2 seconds
            //    Thread.sleep(2000);
            rawEventRepository.save(rawEvent);
            // 2. Compute inline increments
            long impressions = "IMPRESSION".equalsIgnoreCase(message.getEventType()) ? 1L : 0L;
            long clicks = "CLICK".equalsIgnoreCase(message.getEventType()) ? 1L : 0L;

            // 3. Write straight to read-path aggregation table (Postgres UPSERT)
            campaignStatsRepository.upsertStats(message.getCampaignId(), hourBucket.toInstant(ZoneOffset.UTC), impressions, clicks);
            log.info("[PIPELINE SUCCESS] Event parsed, raw logged, and campaign counters updated live!");
            
        } catch (Exception e) {
            // CRITICAL: If the DB crashes, delete the key so Kafka can safely retry the message later
            redisTemplate.delete(deduplicationKey);
            log.error("Failed to save event to Postgres. Cleared idempotency lock for retry.", e);
            throw e; // Rethrow to let Kafka handler know it failed
        }
    }
}