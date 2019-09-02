package com.flex.aws.connection;

import java.util.Collection;
import java.util.List;
import java.util.Map;

import com.amazonaws.client.builder.AwsClientBuilder;
import com.amazonaws.services.dynamodbv2.AmazonDynamoDB;
import com.amazonaws.services.dynamodbv2.AmazonDynamoDBClientBuilder;
import com.amazonaws.services.dynamodbv2.document.BatchWriteItemOutcome;
import com.amazonaws.services.dynamodbv2.document.DynamoDB;
import com.amazonaws.services.dynamodbv2.document.Item;
import com.amazonaws.services.dynamodbv2.document.TableWriteItems;
import com.amazonaws.services.dynamodbv2.model.AttributeDefinition;
import com.amazonaws.services.dynamodbv2.model.AttributeValue;
import com.amazonaws.services.dynamodbv2.model.CreateTableRequest;
import com.amazonaws.services.dynamodbv2.model.DescribeTableRequest;
import com.amazonaws.services.dynamodbv2.model.KeySchemaElement;
import com.amazonaws.services.dynamodbv2.model.KeyType;
import com.amazonaws.services.dynamodbv2.model.ProvisionedThroughput;
import com.amazonaws.services.dynamodbv2.model.PutItemRequest;
import com.amazonaws.services.dynamodbv2.model.PutItemResult;
import com.amazonaws.services.dynamodbv2.model.ScalarAttributeType;
import com.amazonaws.services.dynamodbv2.model.TableDescription;
import com.amazonaws.services.dynamodbv2.model.WriteRequest;
import com.amazonaws.services.dynamodbv2.util.TableUtils;
import com.amazonaws.services.dynamodbv2.util.TableUtils.TableNeverTransitionedToStateException;
import com.fasterxml.jackson.databind.JsonNode;
import com.flex.aws.exceptions.JacksonConverterException;
import com.flex.aws.util.JacksonConverter;
import com.flex.aws.util.JacksonConverterImpl;

public class AWSConnectionManager {

	private AmazonDynamoDB dynamoDB;
	private JacksonConverter converter;
	private DynamoDB dynamoDB2;

	private static AWSConnectionManager instance = null;

	private AWSConnectionManager() {
		/*
		 * ProfileCredentialsProvider credentialsProvider = new
		 * ProfileCredentialsProvider(); try {
		 * credentialsProvider.getCredentials(); } catch (Exception e) { throw
		 * new AmazonClientException(
		 * "Cannot load the credentials from the credential profiles file. " +
		 * "Please make sure that your credentials file is at the correct " +
		 * "location (C:\\Users\\sandesh.indi\\.aws\\credentials), and is in valid format."
		 * , e); } dynamoDB = AmazonDynamoDBClientBuilder.standard()
		 * .withCredentials(credentialsProvider) .withRegion("eu-central-1")
		 * .build();
		 */

		dynamoDB = AmazonDynamoDBClientBuilder.standard().withEndpointConfiguration(
				new AwsClientBuilder.EndpointConfiguration("http://localhost:8000", "eu-central-1")).build();
		dynamoDB2 = new DynamoDB(dynamoDB);
		converter = new JacksonConverterImpl();
	}

	public static AWSConnectionManager getInstance() {
		if (instance == null)
			instance = new AWSConnectionManager();

		return instance;
	}

	public AmazonDynamoDB getDynamoDB() {
		return dynamoDB;
	}
	
	

	public JacksonConverter getConverter() {
		return converter;
	}
	
	
	

	public DynamoDB getDynamoDB2() {
		return dynamoDB2;
	}

	public boolean createTableIfNotExists(String tableName, String keyName, KeyType keyType,
			ScalarAttributeType attributeType, Long readCapacity, Long writeCapacity)
			throws TableNeverTransitionedToStateException, InterruptedException {

		CreateTableRequest createTableRequest = new CreateTableRequest().withTableName(tableName)
				.withKeySchema(new KeySchemaElement().withAttributeName(keyName).withKeyType(keyType))
				.withAttributeDefinitions(
						new AttributeDefinition().withAttributeName(keyName).withAttributeType(attributeType))
				.withProvisionedThroughput(
						new ProvisionedThroughput().withReadCapacityUnits(1L).withWriteCapacityUnits(1L));

		// Create table if it does not exist yet
		TableUtils.createTableIfNotExists(dynamoDB, createTableRequest);
		// wait for the table to move into ACTIVE state
		TableUtils.waitUntilActive(dynamoDB, tableName);

		// Describe our new table
		DescribeTableRequest describeTableRequest = new DescribeTableRequest().withTableName(tableName);
		TableDescription tableDescription = dynamoDB.describeTable(describeTableRequest).getTable();
		System.out.println("Table Description: " + tableDescription);
		return true;
	}

	public PutItemResult putItems(String tableName, JsonNode jsonNode) throws JacksonConverterException {

		Map<String, AttributeValue> item = converter.jsonObjectToMap(jsonNode);
		PutItemRequest putItemRequest = new PutItemRequest(tableName, item);
		PutItemResult putItemResult = dynamoDB.putItem(putItemRequest);
		System.out.println("Result: " + putItemResult);
		return putItemResult;
	}
	
	 public void processBatchItems(String tableName, Collection<Item> items) {
   	  // Add a new item, and delete an existing item, from Thread
       TableWriteItems threadTableWriteItems = new TableWriteItems(tableName)
       .withItemsToPut(items);

       System.out.println("Making the request.");
       BatchWriteItemOutcome outcome = dynamoDB2.batchWriteItem(threadTableWriteItems);

       do {

           // Check for unprocessed keys which could happen if you exceed provisioned throughput

           Map<String, List<WriteRequest>> unprocessedItems = outcome.getUnprocessedItems();

           if (outcome.getUnprocessedItems().size() == 0) {
               System.out.println("No unprocessed items found");
           } else {
               System.out.println("Retrieving the unprocessed items");
               outcome = dynamoDB2.batchWriteItemUnprocessed(unprocessedItems);
           }

       } while (outcome.getUnprocessedItems().size() > 0);
   }

}
