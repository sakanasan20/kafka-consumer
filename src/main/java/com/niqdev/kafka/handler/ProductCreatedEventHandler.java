package com.niqdev.kafka.handler;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.dao.DataIntegrityViolationException;
import org.springframework.http.HttpMethod;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.kafka.annotation.KafkaHandler;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.support.KafkaHeaders;
import org.springframework.messaging.handler.annotation.Header;
import org.springframework.messaging.handler.annotation.Payload;
import org.springframework.stereotype.Component;
import org.springframework.transaction.annotation.Transactional;
import org.springframework.web.client.HttpServerErrorException;
import org.springframework.web.client.ResourceAccessException;
import org.springframework.web.client.RestTemplate;

import com.niqdev.kafka.entity.ProcessedEventEntity;
import com.niqdev.kafka.exception.NotRetryableException;
import com.niqdev.kafka.exception.RetryableException;
import com.niqdev.kafka.repository.ProcessedEventRepository;
import com.niqdev.kafka.shared.event.ProductCreatedEvent;

@Component
@KafkaListener(topics = "product-created-events-topic")
public class ProductCreatedEventHandler {
	
	private final Logger LOGGER = LoggerFactory.getLogger(getClass());

	@Autowired
	RestTemplate restTemplate;
	
	@Autowired
	private ProcessedEventRepository processedEventRepository;

	@Transactional
	@KafkaHandler
	public void handle(@Payload ProductCreatedEvent productCreatedEvent, 
			@Header("messageId") String messageId, 
			@Header(KafkaHeaders.RECEIVED_KEY) String messageKey) {
		
		ProcessedEventEntity existingRecord = processedEventRepository.findByMessageId(messageId);
		
		if (existingRecord != null) {
			LOGGER.info("Found duplicated message id: {}", existingRecord.getMessageId());
			return;
		}
		
		LOGGER.info("Received a new event: " + productCreatedEvent.getTitle());
		
		String url = "http://localhost:8082";
		
		try {
			ResponseEntity<String> response = restTemplate.exchange(url, HttpMethod.GET, null, String.class);
			
			if (response.getStatusCode().value() == HttpStatus.OK.value()) {
				LOGGER.info("Received response from a remote service: " + response.getBody());
			}
		} catch (ResourceAccessException ex) {
			LOGGER.info(ex.getMessage());
			throw new RetryableException(ex);
		} catch (HttpServerErrorException ex) {
			LOGGER.info(ex.getMessage());
			throw new NotRetryableException(ex);
		} catch (Exception ex) {
			LOGGER.info(ex.getMessage());
			throw new NotRetryableException(ex);
		}
		
		try {
			processedEventRepository.save(new ProcessedEventEntity(messageId, productCreatedEvent.getProductId()));
		} catch(DataIntegrityViolationException ex) {
			throw new NotRetryableException(ex);
		}

	}
	
}
