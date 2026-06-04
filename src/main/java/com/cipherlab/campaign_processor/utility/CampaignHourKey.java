package com.cipherlab.campaign_processor.utility;
import java.time.Instant;
public record CampaignHourKey(Long campaignId, Instant hour) {}
