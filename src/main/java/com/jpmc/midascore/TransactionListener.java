package com.jpmc.midascore;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.jpmc.midascore.foundation.Transaction;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;

public class TransactionListener {

    private final ObjectMapper objectMapper = new ObjectMapper();

    @KafkaListener(topics = "${general.kafka-topic}", groupId = "midas-core-group")
    public void listen(String message) {
        try {
            Transaction transaction = objectMapper.readValue(message, Transaction.class);
            System.out.println("Received transaction: " + transaction);
        } catch (Exception e) {
            System.err.println("Failed to deserialize message: " + message);
            e.printStackTrace();
        }
    }
}
