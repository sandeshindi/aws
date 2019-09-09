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
	public static final String SRC_SEAS_EVENT = FlexAwsConstants.getProperty("sourceSeaEvent","SOURCE_SEASON_EVENT");
	public static final String PROD_SEAS_RMV_EVENT = FlexAwsConstants.getProperty("prodSeasonRemoveEvent","PRODUCT_SEASON_REMOVE_EVENT");
	public static final String SKU_SEAS_RMV_EVENT = FlexAwsConstants.getProperty("colorwaySeasRemoveEvent","COLORWAY_SEASON_REMOVE_EVENT");
	public static final String SRC_SEAS_RMV_EVENT = FlexAwsConstants.getProperty("sourceSeasRemoveEvent","SOURCE_SEASON_REMOVE_EVENT");
	public static final String PAYLOAD_EMPTY_MESSAGE =  FlexAwsConstants.getProperty("payloadEmptyMessage");
	public static final String PROD_TABLE_NAME = FlexAwsConstants.getProperty("productTableName");
	public static final String COLORWAY_OBJECT_KEY = FlexAwsConstants.getProperty("colorwayChildObjectKey", "colorways");
	public static final String SOURCES_OBJECT_KEY = FlexAwsConstants.getProperty("sourceChildObjectKey", "sources");
	public static final String PROD_ID_KEY = FlexAwsConstants.getProperty("productIDKey", "id");
	public static AWSConnectionManager dynamoDBMgr = AWSConnectionManager.getInstance();
	
	@SuppressWarnings("unchecked")
	public void loadData(Map<String, Object> data) throws Exception {
		logger.debug("****FlexAWSProductLoader***start");
		String event = (String) data.get("event");
		logger.info(event);
		Map<String,Object> payload = (Map<String, Object>) data.get("payload");
		if(payload == null) {
			throw new FlexAwsExceptions(PAYLOAD_EMPTY_MESSAGE);
		}
		if(PROD_SEAS_EVENT.equals(event)) {
			handleProductSeasonEvent(payload);
		}
		else if(SKU_SEAS_EVENT.equals(event)) {
			handleSkuSeasonEvent(payload);
		}
		else if(SRC_SEAS_EVENT.equals(event)) {
			handleSrcSeasonEvent(payload);
		}
		else if(PROD_SEAS_RMV_EVENT.equals(event)) {
			handleRemoveEvent(payload, null);
		}
		else if (SKU_SEAS_RMV_EVENT.equals(event)) {
			handleRemoveEvent(payload, COLORWAY_OBJECT_KEY);
		}
		else if (SRC_SEAS_RMV_EVENT.equals(event)) {
			handleRemoveEvent(payload, SOURCES_OBJECT_KEY);
		}
	
	
		logger.debug("****FlexAWSProductLoader***end");
	}
	
	public void handleRemoveEvent(Map<String, Object> payload, String childObjectName) {
		logger.debug("****handleRemoveEvent***start");
		String id = (String) payload.get(PROD_ID_KEY);
		String secondaryId = (String) payload.get("secondaryId");
		
		dynamoDBMgr.deleteItem(PROD_TABLE_NAME, PROD_ID_KEY, id, childObjectName, secondaryId);
		
		
		logger.debug("****handleRemoveEvent***end");
	}

	public void handleSrcSeasonEvent(Map<String, Object> payload) throws TableNeverTransitionedToStateException, InterruptedException {
		logger.debug("****handleSrcSeasonEvent***start");
		String id = (String) payload.get(PROD_ID_KEY);
		
		if(FlexAwsUtil.checkIfTableExists(dynamoDBMgr, PROD_TABLE_NAME, "id")) {
			dynamoDBMgr.putItem(PROD_TABLE_NAME, PROD_ID_KEY, id, SOURCES_OBJECT_KEY, payload);
		}
		
		logger.debug("****handleSrcSeasonEvent***end");
		
	}

	public void handleSkuSeasonEvent(Map<String, Object> payload) throws TableNeverTransitionedToStateException, InterruptedException {
		logger.debug("****handleSkuSeasonEvent***start");
		String id = (String) payload.get(PROD_ID_KEY);
		
		if(FlexAwsUtil.checkIfTableExists(dynamoDBMgr, PROD_TABLE_NAME, "id")) {
			dynamoDBMgr.putItem(PROD_TABLE_NAME, PROD_ID_KEY, id, COLORWAY_OBJECT_KEY, payload);
		}
		
		logger.debug("****handleSkuSeasonEvent***end");

		
	}

	public void handleProductSeasonEvent(Map<String, Object> payload) throws TableNeverTransitionedToStateException, InterruptedException, IllegalArgumentException, JacksonConverterException {
		logger.debug("****handleProductSeasonEvent***start");
		String id = (String) payload.get(PROD_ID_KEY);
		if(FlexAwsUtil.checkIfTableExists(dynamoDBMgr, PROD_TABLE_NAME, "id")) {
			dynamoDBMgr.putItem(PROD_TABLE_NAME, PROD_ID_KEY, id, payload);
		}
		
		logger.debug("****handleProductSeasonEvent***end");
	}


	public static void main(String[] args) {
		try {
			Map<String, Object> data = new ObjectMapper().readValue(new File("E:\\testPrdDelete.json"),
					                                   new TypeReference<Map<String, Object>>(){});
			
			new FlexAWSProductLoader().loadData(data);
		} catch (Exception e) {
			// TODO Auto-generated catch block
			logger.catching(e);
		}
	}
	
}
