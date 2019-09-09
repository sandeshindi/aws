package com.flex.aws.util;

import com.amazonaws.services.dynamodbv2.model.KeyType;
import com.amazonaws.services.dynamodbv2.model.ScalarAttributeType;
import com.amazonaws.services.dynamodbv2.util.TableUtils.TableNeverTransitionedToStateException;
import com.flex.aws.connection.AWSConnectionManager;

public class FlexAwsUtil {
	
	public static boolean checkIfTableExists(AWSConnectionManager dbmanager, String tableName, String key) throws TableNeverTransitionedToStateException, InterruptedException {
		boolean tableExists = dbmanager.createTableIfNotExists(tableName, key, KeyType.HASH,
				ScalarAttributeType.S, 1L, 1L);
		return tableExists;
	}

}
