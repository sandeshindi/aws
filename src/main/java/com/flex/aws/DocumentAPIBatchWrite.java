package com.flex.aws;

import java.io.File;
import java.io.IOException;
import java.util.Collection;
import java.util.Date;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Vector;

import com.amazonaws.client.builder.AwsClientBuilder;
import com.amazonaws.services.dynamodbv2.AmazonDynamoDBClientBuilder;
import com.amazonaws.services.dynamodbv2.document.BatchWriteItemOutcome;
import com.amazonaws.services.dynamodbv2.document.DynamoDB;
import com.amazonaws.services.dynamodbv2.document.Item;
import com.amazonaws.services.dynamodbv2.document.ItemUtils;
import com.amazonaws.services.dynamodbv2.document.TableWriteItems;
import com.amazonaws.services.dynamodbv2.model.AttributeValue;
import com.amazonaws.services.dynamodbv2.model.WriteRequest;
import com.fasterxml.jackson.core.JsonFactory;
import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.flex.aws.util.JacksonConverter;
import com.flex.aws.util.JacksonConverterImpl;

public class DocumentAPIBatchWrite {

    static DynamoDB dynamoDB =  new DynamoDB(AmazonDynamoDBClientBuilder.standard().withEndpointConfiguration(
			new AwsClientBuilder.EndpointConfiguration("http://localhost:8000", "eu-central-1")).build());

    static String tableName = "movies-table";
   
    public static void main(String[] args) throws IOException {

        writeMultipleItemsBatchWrite();  

    }

    private static void writeMultipleItemsBatchWrite() {
    	 Date startDate = new Date();
     	
        try {                    
           JsonParser parser = new JsonFactory().createParser(new File("E:\\moviedata.json"));
			JsonNode rootNode = new ObjectMapper().readTree(parser);
			Iterator<JsonNode> iter = rootNode.iterator();
			Collection<Item> items = new Vector<Item>();
			JacksonConverter converter = new JacksonConverterImpl();
			while(iter.hasNext()) {
				JsonNode node = iter.next();
				Map<String, AttributeValue> item = converter.jsonObjectToMap(node);
				items.add(ItemUtils.toItem(item));
				
				if(items.size() == 24) {
					processBatchItems(tableName, items);
					items = new Vector<Item>();
				}
			}
			

          

        }  catch (Exception e) {
            System.err.println("Failed to retrieve items: ");
            e.printStackTrace(System.err);
        }  
        Date endDate = new Date();
        System.out.println("Time taken completion---" + (endDate.getTime() - startDate.getTime()));

    }
    
    public static void processBatchItems(String tableName, Collection<Item> items) {
    	  // Add a new item, and delete an existing item, from Thread
        TableWriteItems threadTableWriteItems = new TableWriteItems(tableName)
        .withItemsToPut(items);

        System.out.println("Making the request.");
        BatchWriteItemOutcome outcome = dynamoDB.batchWriteItem(threadTableWriteItems);

        do {

            // Check for unprocessed keys which could happen if you exceed provisioned throughput

            Map<String, List<WriteRequest>> unprocessedItems = outcome.getUnprocessedItems();

            if (outcome.getUnprocessedItems().size() == 0) {
                System.out.println("No unprocessed items found");
            } else {
                System.out.println("Retrieving the unprocessed items");
                outcome = dynamoDB.batchWriteItemUnprocessed(unprocessedItems);
            }

        } while (outcome.getUnprocessedItems().size() > 0);
    }

}