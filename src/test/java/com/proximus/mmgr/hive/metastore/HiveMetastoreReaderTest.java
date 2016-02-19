package com.proximus.mmgr.hive.metastore;

import static org.junit.Assert.*;
import static org.assertj.core.api.Assertions.contentOf;

import java.io.File;
import java.io.IOException;
import java.util.Properties;
import java.util.logging.LogManager;
import java.security.InvalidParameterException;

import org.apache.hadoop.hive.conf.HiveConf;
import org.junit.Test;

import com.proximus.mmgr.hive.metastore.HiveMetastoreReader;

public class HiveMetastoreReaderTest {

	@Test(expected = IOException.class)
	public void valConfigFileDoesNotExist() throws Exception {
		LogManager.getLogManager().reset();
		File emptyConfig = new File("");
		HiveMetastoreReader.getMetastoreReaderProperties(emptyConfig);
	}
	
	@Test(expected = InvalidParameterException.class)
	public void valNoHiveConfHome() throws Exception {
		LogManager.getLogManager().reset();
		Properties metastoreReaderProperties = new Properties();
		File metastoreReaderConfig = new File("src/test/resources/noHiveConfFile.xml");
		metastoreReaderProperties = HiveMetastoreReader.getMetastoreReaderProperties(metastoreReaderConfig);
		HiveMetastoreReader.checkMetastoreReaderProperties(metastoreReaderProperties);
	}
	
	@Test(expected = InvalidParameterException.class)
	public void valNoAuthenticationMethod() throws Exception {
		LogManager.getLogManager().reset();
		Properties metastoreReaderProperties = new Properties();
		File metastoreReaderConfig = new File("src/test/resources/noAuthenticationMethod.xml");
		metastoreReaderProperties = HiveMetastoreReader.getMetastoreReaderProperties(metastoreReaderConfig);
		HiveMetastoreReader.checkMetastoreReaderProperties(metastoreReaderProperties);
	}
	
	@Test(expected = InvalidParameterException.class)
	public void valNoHiveSite() throws Exception {
		LogManager.getLogManager().reset();
		Properties metastoreReaderProperties = new Properties();
		File metastoreReaderConfig = new File("src/test/resources/noHiveSite.xml");
		metastoreReaderProperties = HiveMetastoreReader.getMetastoreReaderProperties(metastoreReaderConfig);
		HiveMetastoreReader.getHiveConfiguration(metastoreReaderProperties);
	}
}
