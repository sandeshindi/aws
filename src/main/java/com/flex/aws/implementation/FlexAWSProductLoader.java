package com.flex.aws.implementation;

import java.io.File;
import java.util.Map;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.amazonaws.services.dynamodbv2.util.TableUtils.TableNeverTransitionedToStateException;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.flex.aws.FlexAWSConnector;
import com.flex.aws.connection.AWSConnectionManager;
import com.flex.aws.constants.FlexAwsConstants;
import com.flex.aws.exceptions.FlexAwsExceptions;
import com.flex.aws.exceptions.JacksonConverterException;
import com.flex.aws.util.FlexAwsUtil;

public class FlexAWSProductLoader implements FlexAWSConnector {
	public static Logger logger = LogManager.getLogger(FlexAWSProductLoader.class);
	public static final String PROD_SEAS_EVENT = FlexAwsConstants.getProperty("prodSeasEvent","PRODUCT_SEASON_EVENT");
	public static final String SKU_SEAS_EVENT = FlexAwsConstants.getProperty("colorwaySeasEvent","COLORWAY_SEASON_EVENT");
	public static final String PAYLOAD_EMPTY_MESSAGE =  FlexAwsConstants.getProperty("payloadEmptyMessage");
	public static final String PROD_TABLE_NAME = FlexAwsConstants.getProperty("productTableName");
	public static AWSConnectionManager dynamoDBMgr = AWSConnectionManager.getInstance();
	
	@SuppressWarnings("unchecked")
	public void loadData(Map<String, Object> data) throws Exception {
		logger.debug("****FlexAWSProductLoader***start");
		String event = (String) data.get("event");
		logger.info(PROD_SEAS_EVENT);
		
		Map<String,Object> payload = (Map<String, Object>) data.get("payload");
		if(payload == null) {
			throw new FlexAwsExceptions(PAYLOAD_EMPTY_MESSAGE);
		}
		if(PROD_SEAS_EVENT.equals(event)) {
			handleProductSeasonEvent(payload);
		}
		else if(SKU_SEAS_EVENT.equals(event)) {
			System.out.println(SKU_SEAS_EVENT);
			handleSkuSeasonEvent(payload);
		}
	
		logger.debug("****FlexAWSProductLoader***end");
	}
	
	public void handleSkuSeasonEvent(Map<String, Object> payload) throws TableNeverTransitionedToStateException, InterruptedException {
		logger.debug("****handleSkuSeasonEvent***start");
		String id = (String) payload.get("id");
		
		if(FlexAwsUtil.checkIfTableExists(dynamoDBMgr, PROD_TABLE_NAME, "id")) {
			dynamoDBMgr.putItem(PROD_TABLE_NAME, "id", id, "colorways", payload);
		}
		
		logger.debug("****handleSkuSeasonEvent***end");

		
	}

	public void handleProductSeasonEvent(Map<String, Object> payload) throws TableNeverTransitionedToStateException, InterruptedException, IllegalArgumentException, JacksonConverterException {
		logger.debug("****handleProductSeasonEvent***start");
		String id = (String) payload.get("id");
		
		if(FlexAwsUtil.checkIfTableExists(dynamoDBMgr, PROD_TABLE_NAME, "id")) {
			dynamoDBMgr.putItem(PROD_TABLE_NAME, "id", id, payload);
		}
		
		logger.debug("****handleProductSeasonEvent***end");
	}


	public static void main(String[] args) {
		try {
			Map<String, Object> data = new ObjectMapper().readValue(new File("E:\\testColorway.json"),
					                                   new TypeReference<Map<String, Object>>(){});
			
			new FlexAWSProductLoader().loadData(data);
		} catch (Exception e) {
			// TODO Auto-generated catch block
			logger.catching(e);
		}
	}
	
}
