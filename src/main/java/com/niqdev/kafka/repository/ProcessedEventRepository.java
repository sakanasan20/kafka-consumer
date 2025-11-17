package com.niqdev.kafka.repository;

import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.stereotype.Repository;

import com.niqdev.kafka.entity.ProcessedEventEntity;

@Repository
public interface ProcessedEventRepository extends JpaRepository<ProcessedEventEntity, Long> {

	ProcessedEventEntity findByMessageId(String messageId);
	
}
