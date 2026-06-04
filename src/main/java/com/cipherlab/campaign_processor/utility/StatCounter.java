package com.cipherlab.campaign_processor.utility;

public class StatCounter {
    private long clicks = 0;
    private long impressions = 0;

    public void addEvent(String type) {
        if ("CLICK".equalsIgnoreCase(type)) clicks++;
        else if ("IMPRESSION".equalsIgnoreCase(type)) impressions++;
    }

    public void merge(StatCounter other) {
        this.clicks += other.clicks;
        this.impressions += other.impressions;
    }

    public long getClicks() { return clicks; }
    public long getImpressions() { return impressions; }
}