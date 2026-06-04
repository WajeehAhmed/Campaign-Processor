package com.cipherlab.campaign_processor;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.context.annotation.ComponentScan;
import org.springframework.scheduling.annotation.EnableScheduling;

// @EnableScheduling
@SpringBootApplication
@ComponentScan(basePackages = {"com.cipherlab.campaign_processor"})
public class CampaignProcessorApplication {

	public static void main(String[] args) {
		SpringApplication.run(CampaignProcessorApplication.class, args);
	}

}
