package com.niqdev.kafka.handler;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.ArgumentMatchers.isNull;
import static org.mockito.Mockito.timeout;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.math.BigDecimal;
import java.util.UUID;
import java.util.concurrent.ExecutionException;

import org.apache.kafka.clients.producer.ProducerRecord;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.http.HttpHeaders;
import org.springframework.http.HttpMethod;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.support.KafkaHeaders;
import org.springframework.kafka.test.context.EmbeddedKafka;
import org.springframework.test.context.bean.override.mockito.MockitoBean;
import org.springframework.test.context.bean.override.mockito.MockitoSpyBean;
import org.springframework.web.client.RestTemplate;

import com.niqdev.kafka.entity.ProcessedEventEntity;
import com.niqdev.kafka.repository.ProcessedEventRepository;
import com.niqdev.kafka.shared.event.ProductCreatedEvent;

@EmbeddedKafka
@SpringBootTest(properties = "spring.kafka.bootstrap-servers=${spring.embedded.kafka.brokers}")
public class ProductCreatedEventHandlerIntegrationTest {

	@MockitoBean
	RestTemplate restTemplate;
	
	@MockitoBean
	ProcessedEventRepository processedEventRepository;
	
	@Autowired
	KafkaTemplate<String, Object> kafkaTemplate;
	
	@MockitoSpyBean
	ProductCreatedEventHandler productCreatedEventHandler;
	
	@Test
	void testProductCreatedEventHandler_onProductCreated_HandlesEvent() throws InterruptedException, ExecutionException {
	
		// Arrange
		ProductCreatedEvent productCreatedEvent = new ProductCreatedEvent();
		
		String productId = UUID.randomUUID().toString();
		String title = "iPhone 11";
		BigDecimal price = new BigDecimal(600);
		Integer quantity = 1;
		
		productCreatedEvent.setProductId(productId);
		productCreatedEvent.setTitle(title);
		productCreatedEvent.setPrice(price);
		productCreatedEvent.setQuantity(quantity);
		
		String messageId = UUID.randomUUID().toString();
		String messageKey = productCreatedEvent.getProductId();
		
		ProducerRecord<String, Object> record = new ProducerRecord<>(
				"product-created-events-topic", 
				messageKey, 
				productCreatedEvent);
		
		record.headers().add("messageId", messageId.getBytes());
		record.headers().add(KafkaHeaders.RECEIVED_KEY, messageKey.getBytes());
		
		ProcessedEventEntity processedEventEntity = new ProcessedEventEntity();
		when(processedEventRepository.findByMessageId(anyString())).thenReturn(processedEventEntity);
		when(processedEventRepository.save(any(ProcessedEventEntity.class))).thenReturn(null);
		
		String responseBody = "{\"key\":\"value\"}";
		HttpHeaders httpHeaders = new HttpHeaders();
		ResponseEntity<String> responseEntity = new ResponseEntity<>(responseBody, httpHeaders, HttpStatus.OK);
		
		when(restTemplate.exchange(
				any(String.class), 
				any(HttpMethod.class), 
				isNull(), 
				eq(String.class)))
		.thenReturn(responseEntity);
		
		// Act
		kafkaTemplate.send(record).get();
		
		// Arrange
		ArgumentCaptor<String> messageIdCaptor = ArgumentCaptor.forClass(String.class);
		ArgumentCaptor<String> messageKeyCaptor = ArgumentCaptor.forClass(String.class);
		ArgumentCaptor<ProductCreatedEvent> eventCaptor = ArgumentCaptor.forClass(ProductCreatedEvent.class);
		
		verify(productCreatedEventHandler, timeout(5000).times(1)).handle(eventCaptor.capture(), 
				messageIdCaptor.capture(), 
				messageKeyCaptor.capture());
		
		assertEquals(messageId, messageIdCaptor.getValue());
		assertEquals(messageKey, messageKeyCaptor.getValue());
		assertEquals(productCreatedEvent.getProductId(), eventCaptor.getValue().getProductId());
	}
	
}
