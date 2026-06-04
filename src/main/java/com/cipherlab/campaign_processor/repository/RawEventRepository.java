package com.cipherlab.campaign_processor.repository;

import org.springframework.data.jpa.repository.JpaRepository;

import com.cipherlab.campaign_processor.entity.RawEvent;

import java.time.Instant;
import java.util.List;
import java.util.UUID;

public interface RawEventRepository extends JpaRepository<RawEvent, UUID> {
    List<RawEvent> findByTimestampGreaterThanAndTimestampLessThan(Instant start, Instant end);
}