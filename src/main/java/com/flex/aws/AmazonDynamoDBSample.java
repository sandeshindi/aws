package com.flex.aws;

import java.io.File;
import java.util.Collection;
import java.util.Date;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Vector;

import com.amazonaws.AmazonClientException;
import com.amazonaws.AmazonServiceException;
import com.amazonaws.services.dynamodbv2.document.Item;
import com.amazonaws.services.dynamodbv2.document.ItemCollection;
import com.amazonaws.services.dynamodbv2.document.ItemUtils;
import com.amazonaws.services.dynamodbv2.document.ScanOutcome;
import com.amazonaws.services.dynamodbv2.document.Table;
import com.amazonaws.services.dynamodbv2.document.spec.ScanSpec;
import com.amazonaws.services.dynamodbv2.document.utils.NameMap;
import com.amazonaws.services.dynamodbv2.document.utils.ValueMap;
import com.amazonaws.services.dynamodbv2.model.AttributeValue;
import com.amazonaws.services.dynamodbv2.model.ComparisonOperator;
import com.amazonaws.services.dynamodbv2.model.Condition;
import com.amazonaws.services.dynamodbv2.model.KeyType;
import com.amazonaws.services.dynamodbv2.model.ScalarAttributeType;
import com.amazonaws.services.dynamodbv2.model.ScanRequest;
import com.amazonaws.services.dynamodbv2.model.ScanResult;
import com.fasterxml.jackson.core.JsonFactory;
import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.flex.aws.connection.AWSConnectionManager;
import com.flex.aws.exceptions.JacksonConverterException;
import com.flex.aws.util.JacksonConverter;
import com.flex.aws.util.JacksonConverterImpl;

/**
 * This sample demonstrates how to perform a few simple operations with the
 * Amazon DynamoDB service.
 */
public class AmazonDynamoDBSample {

	/*
	 * Before running the code: Fill in your AWS access credentials in the
	 * provided credentials file template, and be sure to move the file to the
	 * default location (C:\\Users\\sandesh.indi\\.aws\\credentials) where the
	 * sample code will load the credentials from.
	 * https://console.aws.amazon.com/iam/home?#security_credential
	 *
	 * WARNING: To avoid accidental leakage of your credentials, DO NOT keep the
	 * credentials file in your source directory.
	 */

	static AWSConnectionManager dynamoDBMgr;

	/**
	 * The only information needed to create a client are security credentials
	 * consisting of the AWS Access Key ID and Secret Access Key. All other
	 * configuration, such as the service endpoints, are performed
	 * automatically. Client parameters, such as proxies, can be specified in an
	 * optional ClientConfiguration object when constructing a client.
	 *
	 * @see com.amazonaws.auth.BasicAWSCredentials
	 * @see com.amazonaws.auth.ProfilesConfigFile
	 * @see com.amazonaws.ClientConfiguration
	 */
	private static void init() throws Exception {
		/*
		 * The ProfileCredentialsProvider will return your [default] credential
		 * profile by reading from the credentials file located at
		 * (C:\\Users\\sandesh.indi\\.aws\\credentials).
		 */
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

		dynamoDBMgr = AWSConnectionManager.getInstance();
	}

	public static void main(String[] args) throws Exception {
		init();

		try {
			String tableName = "product-data";

			boolean tableExists = dynamoDBMgr.createTableIfNotExists(tableName, "workingNumber", KeyType.HASH,
					ScalarAttributeType.S, 1L, 1L);
			if (tableExists) {
				long start = System.currentTimeMillis();
				
				JsonParser parser = new JsonFactory().createParser(new File("E:\\moviedata.json"));
				JsonNode rootNode = new ObjectMapper().readTree(parser);
				Iterator<JsonNode> iter = rootNode.iterator();
				
				iter.forEachRemaining((JsonNode node) -> {
					try {
						dynamoDBMgr.putItems(tableName, node);
					} catch (JacksonConverterException e) {
						System.out.println("Error Message: " + e.getMessage());
					}
				});
				long end = System.currentTimeMillis();
				
				
				// Scan items for movies with a year attribute greater than 1985
				/*
				 * HashMap<String, Condition> scanFilter = new HashMap<String, Condition>();
				 * Condition condition = new
				 * Condition().withComparisonOperator(ComparisonOperator.EQ.toString())
				 * .withAttributeValueList(new AttributeValue().withS("LM43547"));
				 * scanFilter.put("workingNumber", condition);
				 * 
				 * ScanRequest scanRequest = new
				 * ScanRequest(tableName).withScanFilter(scanFilter); ScanResult scanResult =
				 * dynamoDBMgr.getDynamoDB().scan(scanRequest); List<Map<String,
				 * AttributeValue>> itemsResult = scanResult.getItems(); Iterator<Map<String,
				 * AttributeValue>> iterator = itemsResult.iterator(); Map<String,
				 * AttributeValue> item = null; while (iterator.hasNext()) { item =
				 * iterator.next(); System.out.println(ItemUtils.toItem(item).toJSONPretty()); }
				 */
				
				Table table = dynamoDBMgr.getDynamoDB2().getTable(tableName);
				ScanSpec scanSpec = new ScanSpec().withProjectionExpression("season")
											  .withFilterExpression("workingNumber = :workingNumber")
						                      .withValueMap(new ValueMap().with(":workingNumber", "LM43547"));

				ItemCollection<ScanOutcome> items = table.scan(scanSpec);
				Iterator<Item> itera = items.iterator();
		        while (itera.hasNext()) {
		            Item item = itera.next();
		            System.out.println(item.toJSONPretty());
		        }
				System.out.println("Time taken completion---" + ((end - start)/1000F)/60F);
				
			}

		} catch (AmazonServiceException ase) {
			System.out.println("Caught an AmazonServiceException, which means your request made it "
					+ "to AWS, but was rejected with an error response for some reason.");
			System.out.println("Error Message:    " + ase.getMessage());
			System.out.println("HTTP Status Code: " + ase.getStatusCode());
			System.out.println("AWS Error Code:   " + ase.getErrorCode());
			System.out.println("Error Type:       " + ase.getErrorType());
			System.out.println("Request ID:       " + ase.getRequestId());
		} catch (AmazonClientException ace) {
			System.out.println("Caught an AmazonClientException, which means the client encountered "
					+ "a serious internal problem while trying to communicate with AWS, "
					+ "such as not being able to access the network.");
			System.out.println("Error Message: " + ace.getMessage());
		} 
	}
}
