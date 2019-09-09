package com.flex.aws;

import java.io.File;
import java.io.IOException;
import java.util.Map;

import org.junit.Test;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.flex.aws.implementation.FlexAWSProductLoader;


public class FlexAWSProductLoaderTest {
	FlexAWSProductLoader loader;
	
	public FlexAWSProductLoaderTest() {
		loader = new FlexAWSProductLoader();
	}
	
	@Test
	public void handleProductOperations() {
		try {
			Map<String, Object> data = new ObjectMapper().readValue(new File("..\\..\\..\\..\\resources\\testProduct.json"),
			        new TypeReference<Map<String, Object>>(){});
			loader.loadData(data);
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}
	
	@Test
	public void handleColorwayOperations() {
		try {
			Map<String, Object> data = new ObjectMapper().readValue(new File("..\\..\\..\\..\\resources\\testColorway.json"),
			        new TypeReference<Map<String, Object>>(){});
			loader.loadData(data);
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}
	
	@Test
	public void handleSourceOperations() {
		try {
			Map<String, Object> data = new ObjectMapper().readValue(new File("..\\..\\..\\..\\resources\\testSources.json"),
			        new TypeReference<Map<String, Object>>(){});
			loader.loadData(data);
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}
	
	@Test
	public void handleSourceRemoveOperations() {
		try {
			Map<String, Object> data = new ObjectMapper().readValue(new File("..\\..\\..\\..\\resources\\testSrcDelete.json"),
			        new TypeReference<Map<String, Object>>(){});
			loader.loadData(data);
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}
	
	@Test
	public void handleColorwayRemoveOperations() {
		try {
			Map<String, Object> data = new ObjectMapper().readValue(new File("..\\..\\..\\..\\resources\\testCwDelete.json"),
			        new TypeReference<Map<String, Object>>(){});
			loader.loadData(data);
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}
	
	
	@Test
	public void handleProductRemoveOperations() {
		try {
			Map<String, Object> data = new ObjectMapper().readValue(new File("..\\..\\..\\..\\resources\\testPrdDelete.json"),
			        new TypeReference<Map<String, Object>>(){});
			loader.loadData(data);
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}

}
