package com.cipherlab.campaign_processor.config;

import lombok.extern.slf4j.Slf4j;

import java.util.HashMap;
import java.util.Map;

import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.StringSerializer;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.config.ConcurrentKafkaListenerContainerFactory;
import org.springframework.kafka.core.ConsumerFactory;
import org.springframework.kafka.core.DefaultKafkaProducerFactory;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.core.ProducerFactory;
import org.springframework.kafka.listener.DeadLetterPublishingRecoverer;
import org.springframework.kafka.listener.DefaultErrorHandler;
import org.springframework.kafka.support.serializer.JsonSerializer;
import org.springframework.util.backoff.FixedBackOff;


@Configuration
@Slf4j
public class KafkaConsumerConfig {
    // 1. Force the Listener Factory to actively use our Fault-Tolerance settings
    @Bean
    public ConcurrentKafkaListenerContainerFactory<String, Object> kafkaListenerContainerFactory(
            ConsumerFactory<String, Object> consumerFactory,
            DefaultErrorHandler errorHandler) {
        
        ConcurrentKafkaListenerContainerFactory<String, Object> factory = 
                new ConcurrentKafkaListenerContainerFactory<>();
        
        factory.setConsumerFactory(consumerFactory);
        // Bind our retry/DLQ logic directly to the container threads
        factory.setCommonErrorHandler(errorHandler); 
        return factory;
    }
    @Bean
    public DefaultErrorHandler errorHandler(KafkaTemplate<String, Object> template) {
        log.info("Configuring Distributed Fault-Tolerance Retry & DLQ Layer...");

        // 1. Send exhausted failure records straight to the default topic suffix: topic-name.DLT
        DeadLetterPublishingRecoverer recoverer = new DeadLetterPublishingRecoverer(template);

        // 2. Retry policy: Attempt 3 times max, pausing 2 seconds between attempts
        FixedBackOff backOff = new FixedBackOff(2000L, 2L);

        // 3. Create the handler wrapper
        DefaultErrorHandler errorHandler = new DefaultErrorHandler(recoverer, backOff);

        // Optional: Add a log hook to watch the failure delegation happen
        errorHandler.setRetryListeners((record, ex, deliveryAttempt) -> {
            log.warn("[RETRY LOGIC] Failure occurred. Delivery attempt #{} for key: {}. Retrying...", 
                    deliveryAttempt, record.key());
        });

        return errorHandler;
    }

    @Bean
    public KafkaTemplate<String, Object> dlqKafkaTemplate() {
        return new KafkaTemplate<>(dlqProducerFactory());
    }

    private ProducerFactory<String, Object> dlqProducerFactory() {
        Map<String, Object> configProps = new HashMap<>();
        configProps.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:29092");
        configProps.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
        
        // THE CRITICAL FIX: Use JsonSerializer for values instead of StringSerializer
        configProps.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, JsonSerializer.class);
        
        return new DefaultKafkaProducerFactory<>(configProps);
    }
}