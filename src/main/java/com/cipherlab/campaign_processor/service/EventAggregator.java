// package com.cipherlab.campaign_processor.service;

// import java.time.Instant;
// import java.time.temporal.ChronoUnit;
// import java.util.List;
// import java.util.Map;
// import java.util.stream.Collector;
// import java.util.stream.Collectors;

// import org.springframework.data.redis.core.StringRedisTemplate;
// import org.springframework.scheduling.annotation.Scheduled;
// import org.springframework.stereotype.Service;

// import com.cipherlab.campaign_processor.utility.CampaignHourKey;
// import com.cipherlab.campaign_processor.utility.StatCounter;
// import com.cipherlab.campaign_processor.entity.RawEvent;
// import com.cipherlab.campaign_processor.repository.CampaignStatsRepository;
// import com.cipherlab.campaign_processor.repository.RawEventRepository;

// import lombok.RequiredArgsConstructor;
// import lombok.extern.slf4j.Slf4j;

// @Service
// @RequiredArgsConstructor
// @Slf4j
// public class EventAggregator {

//     private final RawEventRepository rawRepo;
//     private final CampaignStatsRepository statsRepo;
//     private final StringRedisTemplate redisTemplate; // Injected Redis client

//     private static final String WATERMARK_KEY = "aggregator:last_processed_ts";

//     @Scheduled(fixedRate = 60000)
//     public void aggregateEvents() {
//         // 1. Get the Watermark from Redis (or default to 1 day ago if empty)
//         String lastTsStr = redisTemplate.opsForValue().get(WATERMARK_KEY);
//         Instant lastProcessed = (lastTsStr != null) 
//             ? Instant.parse(lastTsStr) 
//             : Instant.now().minus(1, ChronoUnit.DAYS);

//         // 2. Set the end of the current window
//         Instant batchEnd = Instant.now().truncatedTo(ChronoUnit.MINUTES);
        
//         log.info("Aggregating from {} to {}", lastProcessed, batchEnd);

//         // 3. Fetch data
//         List<RawEvent> events = rawRepo.findByTimestampGreaterThanAndTimestampLessThan(lastProcessed, batchEnd);

//         if (events.isEmpty()) {
//             // Even if empty, move watermark forward to 'batchEnd' to skip empty time ranges
//             redisTemplate.opsForValue().set(WATERMARK_KEY, batchEnd.toString());
//             return;
//         }

//         // 4. Grouping Logic (The Map-Reduce part we discussed)
//         Map<CampaignHourKey, StatCounter> grouped = events.stream()
//             .collect(Collectors.groupingBy(
//                 // Classifier: Must take a RawEvent and return the Key
//                 event -> new CampaignHourKey(
//                     event.getCampaignId(), 
//                     event.getTimestamp().truncatedTo(ChronoUnit.HOURS)
//                 ),
//                 // Collector: Must handle the RawEvent type
//                 Collector.of(
//                     StatCounter::new, // Supplier
//                     (counter, event) -> counter.addEvent(event.getEventType()), // Accumulator (event is RawEvent)
//                     (left, right) -> { left.merge(right); return left; } // Combiner
//                 )
//             ));

//         // 5. Database Sync (The Upsert part we discussed)
//         grouped.forEach((key, counter) -> {
//             statsRepo.upsertStats(key.campaignId(), key.hour(), counter.getClicks(), counter.getImpressions());
//         });

//         // 6. Persist the new Watermark to Redis
//         redisTemplate.opsForValue().set(WATERMARK_KEY, batchEnd.toString());
        
//         log.info("Processed {} events. Watermark updated to {}", events.size(), batchEnd);
//     }
// }