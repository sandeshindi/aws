package com.flex.aws.constants;

import java.io.IOException;
import java.io.InputStream;
import java.util.Properties;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.flex.aws.exceptions.PropertyException;

public class FlexAwsConstants {
	public static Logger logger = LogManager.getLogger(FlexAwsConstants.class);
	
	static Properties prop = new Properties();
	static String propFileName = "application.properties";
	static {
		InputStream inputStream = FlexAwsConstants.class.getClassLoader().getResourceAsStream(propFileName);
		if (inputStream != null) {
			try {
				prop.load(inputStream);
			} catch (IOException e) {
				logger.catching(e);
			}
		} 
	}
	
	public static String getProperty(String propName) {
		String renderedProps = prop.getProperty(propName);
		
		if(renderedProps != null) {
			return renderedProps;
		}
		else {
			logger.catching(new PropertyException(propName));
			return null;
		}
	}
	
	public static String  getProperty(String propName, String defaultVal) {
		String renderedProps = prop.getProperty(propName, defaultVal);
		
		return renderedProps;
	}
	
	
	

}
