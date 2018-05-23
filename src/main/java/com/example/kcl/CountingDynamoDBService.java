package com.example.kcl;

import com.amazonaws.services.dynamodbv2.AmazonDynamoDB;
import com.amazonaws.services.dynamodbv2.AmazonDynamoDBClientBuilder;
import com.amazonaws.services.dynamodbv2.document.DynamoDB;
import com.amazonaws.services.dynamodbv2.document.Table;
import com.google.gson.JsonObject;
import org.springframework.stereotype.Component;

public class CountingDynamoDBService {

    private static final String COUNTING_SERVICE_TABLE = "CountingServiceTest";

    AmazonDynamoDB client;
    DynamoDB dynamoDB;
    Table table;

    public CountingDynamoDBService() {
        client = AmazonDynamoDBClientBuilder.standard().build();
        dynamoDB = new DynamoDB(client);
        table = dynamoDB.getTable(COUNTING_SERVICE_TABLE);
    }

    public void UpdateReport(String reportKey, JsonObject reportData) {

    }

    public void InsertReport(String reportKey, JsonObject reportData) {

    }
}
