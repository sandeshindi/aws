package com.flex.aws.connection;

import java.util.HashMap;
import java.util.Map;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.amazonaws.client.builder.AwsClientBuilder;
import com.amazonaws.services.dynamodbv2.AmazonDynamoDB;
import com.amazonaws.services.dynamodbv2.AmazonDynamoDBClientBuilder;
import com.amazonaws.services.dynamodbv2.document.DeleteItemOutcome;
import com.amazonaws.services.dynamodbv2.document.DynamoDB;
import com.amazonaws.services.dynamodbv2.document.Item;
import com.amazonaws.services.dynamodbv2.document.ItemUtils;
import com.amazonaws.services.dynamodbv2.document.PrimaryKey;
import com.amazonaws.services.dynamodbv2.document.Table;
import com.amazonaws.services.dynamodbv2.document.spec.DeleteItemSpec;
import com.amazonaws.services.dynamodbv2.document.spec.GetItemSpec;
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
import com.amazonaws.services.dynamodbv2.util.TableUtils;
import com.amazonaws.services.dynamodbv2.util.TableUtils.TableNeverTransitionedToStateException;

public class AWSConnectionManager {

	private AmazonDynamoDB dynamoDB;
	private DynamoDB dynamoDB2;
	public static Logger logger = LogManager.getLogger(AWSConnectionManager.class);

	private static AWSConnectionManager instance = null;

	private AWSConnectionManager() {
		
		/*  ProfileCredentialsProvider credentialsProvider = new ProfileCredentialsProvider(); 
		  try {
		  credentialsProvider.getCredentials(); 
		  } catch (Exception e) { 
			  throw new AmazonClientException("Cannot load the credentials from the credential profiles file. " +
		                                       "Please make sure that your credentials file is at the correct " +
		                                        "location (C:\\Users\\sandesh.indi\\.aws\\credentials), and is in valid format." , e); }
		  dynamoDB = AmazonDynamoDBClientBuilder.standard().withCredentials(credentialsProvider) .withRegion("eu-central-1").build();*/
		 

		dynamoDB = AmazonDynamoDBClientBuilder.standard().withEndpointConfiguration(
				new AwsClientBuilder.EndpointConfiguration("http://localhost:8000", "eu-central-1")).build();
		dynamoDB2 = new DynamoDB(dynamoDB);
	}

	public static AWSConnectionManager getInstance() {
		if (instance == null)
			instance = new AWSConnectionManager();

		return instance;
	}
	
	

	public boolean createTableIfNotExists(String tableName, String keyName, KeyType keyType,
			ScalarAttributeType attributeType, Long readCapacity, Long writeCapacity)
			throws TableNeverTransitionedToStateException, InterruptedException {
		try {
		TableDescription tableDescription = dynamoDB2.getTable(tableName).describe();
        logger.info("Table description: " + tableDescription.getTableStatus());
        return true;
		}
		catch(com.amazonaws.services.dynamodbv2.model.ResourceNotFoundException rnfe) {
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
	}

	public PutItemResult putItem(String tableName, String idKey, String id, Map<String, Object> payload) {
		logger.debug("tableName:" + tableName);
		logger.debug("idKey:" + idKey + "** id:" + id);
		logger.debug("payload:" + payload);
		Item item = this.getItem(tableName, idKey, id);
		logger.debug("item:" + item);
		Map<String, Object> data = item == null ? new HashMap<String, Object>() : item.asMap();
		data.putAll(payload);
		logger.debug("data:" + data);
		PutItemResult putItemResult = putItem(tableName, data);
		return putItemResult;

	}
	
	@SuppressWarnings({ "unchecked" })
	public PutItemResult putItem(String tableName, String idKey, String id, String childObjectName, Map<String, Object> payload) {
		logger.debug("tableName:" + tableName);
		logger.debug("idKey:" + idKey + "** id:" + id + "** childObjectName:" + childObjectName);
		logger.debug("payload:" + payload);
		Item item = this.getItem(tableName, idKey, id);
		logger.debug("item:" + item);
		Map<String, Object> data = item == null ? new HashMap<String, Object>(payload) : item.asMap();
		Map<String, Object> childObjectMap = (Map<String, Object>) data.get(childObjectName);
		if(childObjectMap == null) {
			childObjectMap = new HashMap<String, Object>();
			childObjectMap.putAll((Map<String, Object>) payload.get(childObjectName));
    	}
		else {
		    final Map<String,Object> secondaryKeyMap = new HashMap<String,Object>(childObjectMap);
		    Map<String, Object> payloadSecondarMap = (Map<String, Object>) payload.get(childObjectName);
		    payloadSecondarMap.forEach((key,value) -> {
		    	try{
		    	((Map<String, Object>) secondaryKeyMap.get(key)).putAll((Map<? extends String, ? extends Object>) value);
		    	}
		    	catch(NullPointerException e) {
		    		logger.info("New Child object added--" + payloadSecondarMap.get(key));
		    		Map<String, Object> newChildObject = new HashMap<String, Object>();
		    		newChildObject.putAll((Map<? extends String, ? extends Object>) value);
		    		secondaryKeyMap.put(key, newChildObject);
 		    	}
		    });
		    childObjectMap.putAll(secondaryKeyMap);
		}
		
		data.put(childObjectName, childObjectMap);
		logger.debug("data:" + data);
		PutItemResult putItemResult = putItem(tableName, data);
		return putItemResult;
		
	}
	
	public PutItemResult putItem(String tableName, Map<String,Object> data) {
		Map<String, AttributeValue> itemData = ItemUtils.fromSimpleMap(data);
		PutItemRequest putItemRequest = new PutItemRequest(tableName, itemData);
		PutItemResult putItemResult = dynamoDB.putItem(putItemRequest);
		logger.debug("Result: " + putItemResult.hashCode());
		return putItemResult;
	}
	
	public Item getItem(String tableName, String idKey, String id) {
		GetItemSpec spec = new GetItemSpec().withPrimaryKey(idKey, id);
		Item item = dynamoDB2.getTable(tableName).getItem(spec);
		return item;
	}
	
	@SuppressWarnings("unchecked")
	public int deleteItem(String tableName, String idKey, String id, String childObjectName, String secondaryId) {
		logger.debug("tableName:" + tableName);
		logger.debug("idKey:" + idKey + "** id:" + id + "** childObjectName:" + childObjectName);
		logger.debug("secondaryId:" + secondaryId);
		Table table = dynamoDB2.getTable(tableName);
		if(childObjectName != null) {
			Item item = this.getItem(tableName, idKey, id);
			logger.debug("item:" + item);
			Map<String, Object> itemMap = item.asMap();
			Map<String, Object> childMap = (Map<String, Object>) itemMap.get(childObjectName);
			childMap.remove(secondaryId);
			itemMap.put(childObjectName, childMap);
			logger.debug("data:" + itemMap);
			PutItemResult putItemResult = putItem(tableName, itemMap);
			return putItemResult.hashCode();
		}
		else {
			 DeleteItemSpec deleteItemSpec = new DeleteItemSpec().withPrimaryKey(new PrimaryKey(idKey, id));
			 DeleteItemOutcome outcome = table.deleteItem(deleteItemSpec);
			 logger.debug("Delete Result: " + outcome.hashCode());
			 return outcome.hashCode();
		}
	}
	
   /*	
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
   */

}
