package com.niqdev.kafka.handler;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.HttpMethod;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.kafka.annotation.KafkaHandler;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;
import org.springframework.web.client.HttpServerErrorException;
import org.springframework.web.client.ResourceAccessException;
import org.springframework.web.client.RestTemplate;

import com.niqdev.kafka.exception.NotRetryableException;
import com.niqdev.kafka.exception.RetryableException;
import com.niqdev.kafka.shared.event.ProductCreatedEvent;

@Component
@KafkaListener(topics = "product-created-events-topic")
public class ProductCreatedEventHandler {
	
	private final Logger LOGGER = LoggerFactory.getLogger(getClass());

	@Autowired
	RestTemplate restTemplate;

	@KafkaHandler
	public void handle(ProductCreatedEvent productCreatedEvent) {
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

	}
	
}
