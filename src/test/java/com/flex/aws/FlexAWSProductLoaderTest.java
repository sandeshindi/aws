package com.flex.aws;

import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;

import java.io.File;
import java.io.IOException;
import java.util.Map;

import org.junit.Test;

import com.amazonaws.services.dynamodbv2.document.Item;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.flex.aws.connection.AWSConnectionManager;
import com.flex.aws.constants.FlexAwsConstants;
import com.flex.aws.exceptions.FlexAwsExceptions;
import com.flex.aws.implementation.FlexAWSProductLoader;


public class FlexAWSProductLoaderTest {
	public static final String PROD_SEAS_EVENT = FlexAwsConstants.getProperty("prodSeasEvent1");
	
	FlexAWSProductLoader loader;
	
	
	
	public FlexAWSProductLoaderTest() {
		loader = new FlexAWSProductLoader();
	}
	
	@Test
	public void handleProductOperations() {
		try {
			System.out.println(System.getProperty("user.dir"));

			Map<String, Object> data = new ObjectMapper().readValue(new File(System.getProperty("user.dir") + File.separator + 
					                                                 "target" + File.separator + "test-classes" + File.separator + "testProduct.json"),
			        new TypeReference<Map<String, Object>>(){});
			loader.loadData(data);
			assertNotNull(AWSConnectionManager.getInstance().getItem(FlexAwsConstants.getProperty("productTableName"), "id", "201949-W201927"));
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}
	
	@SuppressWarnings("unchecked")
	@Test
	public void handleColorwayOperations() {
		try {
			Map<String, Object> data = new ObjectMapper().readValue(new File(System.getProperty("user.dir") + File.separator + 
					                                                 "target" + File.separator + "test-classes" + File.separator + "testColorway.json"),
			        new TypeReference<Map<String, Object>>(){});
			loader.loadData(data);
			Item mainItem = AWSConnectionManager.getInstance().getItem(FlexAwsConstants.getProperty("productTableName"), "id", "201949-W201927");
			Map<String,Object> colorways =  (Map<String, Object>) mainItem.asMap().get("colorways");
			assertNotNull(colorways.get("201949-W201927-A201849"));
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}
	
	@SuppressWarnings("unchecked")
	@Test
	public void handleSourceOperations() {
		try {
			Map<String, Object> data = new ObjectMapper().readValue(new File(System.getProperty("user.dir") + File.separator + 
                    "target" + File.separator + "test-classes" + File.separator + "testSources.json"),
			        new TypeReference<Map<String, Object>>(){});
			loader.loadData(data);
			Item mainItem = AWSConnectionManager.getInstance().getItem(FlexAwsConstants.getProperty("productTableName"), "id", "201949-W201927");
			Map<String,Object> sources =  (Map<String, Object>) mainItem.asMap().get("sources");
			assertNotNull(sources.get("201949-W201927-1SRC1"));
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}
	
	@Test
	public void handleRemoveOperations() {
		try {
			Map<String, Object> data = new ObjectMapper().readValue(new File(System.getProperty("user.dir") + File.separator + 
                    "target" + File.separator + "test-classes" + File.separator + "testSrcDelete.json"),
			        new TypeReference<Map<String, Object>>(){});
			loader.loadData(data);
			data = new ObjectMapper().readValue(new File(System.getProperty("user.dir") + File.separator + 
	                    "target" + File.separator + "test-classes" + File.separator + "testCwDelete.json"),
				        new TypeReference<Map<String, Object>>(){});
			loader.loadData(data);
			data = new ObjectMapper().readValue(new File(System.getProperty("user.dir") + File.separator + 
                    "target" + File.separator + "test-classes" + File.separator + "testPrdDelete.json"),
			        new TypeReference<Map<String, Object>>(){});
			loader.loadData(data);
			assertNull(AWSConnectionManager.getInstance().getItem(FlexAwsConstants.getProperty("productTableName"), "id", "201949-W201927"));
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}
	
	@Test
	public void handleProductScenariosOther() {
		try {
			Map<String, Object> data = new ObjectMapper().readValue(new File(System.getProperty("user.dir") + File.separator + 
                    "target" + File.separator + "test-classes" + File.separator + "testProduct-1.json"),
			        new TypeReference<Map<String, Object>>(){});
			loader.loadData(data);
			data = new ObjectMapper().readValue(new File(System.getProperty("user.dir") + File.separator + 
	                    "target" + File.separator + "test-classes" + File.separator + "testProduct-2.json"),
				        new TypeReference<Map<String, Object>>(){});
			loader.loadData(data);
			data = new ObjectMapper().readValue(new File(System.getProperty("user.dir") + File.separator + 
                    "target" + File.separator + "test-classes" + File.separator + "testEmpty.json"),
			        new TypeReference<Map<String, Object>>(){});
			try {
			loader.loadData(data);
			}
			catch(FlexAwsExceptions e) {
				e.getLocalizedMessage();
			}
			data = new ObjectMapper().readValue(new File(System.getProperty("user.dir") + File.separator + 
                    "target" + File.separator + "test-classes" + File.separator + "testColorway-1.json"),
			        new TypeReference<Map<String, Object>>(){});
			loader.loadData(data);
			data = new ObjectMapper().readValue(new File(System.getProperty("user.dir") + File.separator + 
                    "target" + File.separator + "test-classes" + File.separator + "testColorway-2.json"),
			        new TypeReference<Map<String, Object>>(){});
			loader.loadData(data);
			AWSConnectionManager.getInstance().deleteItem(FlexAwsConstants.getProperty("productTableName"), "id", "201949-W201928", null, null);
			assertNull(AWSConnectionManager.getInstance().getItem(FlexAwsConstants.getProperty("productTableName"), "id", "201949-W201928"));
			
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}
	
	
}
