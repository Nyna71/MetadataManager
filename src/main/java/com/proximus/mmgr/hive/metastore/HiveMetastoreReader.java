package com.proximus.mmgr.hive.metastore;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileWriter;
import java.io.IOException;
import java.util.List;
import java.util.Properties;
import java.util.logging.Level;
import java.util.logging.Logger;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.metastore.HiveMetaStoreClient;
import org.apache.hadoop.hive.metastore.api.FieldSchema;
import org.apache.hadoop.hive.metastore.api.MetaException;
import org.apache.hadoop.hive.metastore.api.NoSuchObjectException;
import org.apache.hadoop.hive.metastore.api.StorageDescriptor;
import org.apache.hadoop.hive.metastore.api.Table;
import org.apache.thrift.TException;


/**
 * Reads the Hive Metastore repository using the Hive Metastore api, and exports Hive Metadata into a set of
 * csv files ready to be uploaded in Informatica Metadata Manager.
 * @author Jonathan Puvilland
 *
 */
public class HiveMetastoreReader {
	private static final String PROPERTIES_FILE = "./etc/HiveMetastoreConfig.xml";
	private static final Logger logger = Logger.getLogger(HiveMetastoreReader.class.getName());

	public static void main(String[] args) throws Exception
	{
		Properties hiveMetastoreProps = new Properties();
		File propertiesFile = new File(PROPERTIES_FILE);
		retrieveProperties(hiveMetastoreProps, propertiesFile);
		
		try {
			HiveMetaStoreClient hiveClient = new HiveMetaStoreClient(getHiveConfiguration(hiveMetastoreProps));

	        File file = new File(hiveMetastoreProps.getProperty("metastore_output_file"));
			BufferedWriter hiveCatalogOutput = new BufferedWriter(new FileWriter(file));
	    	exportDatabases(hiveClient, hiveCatalogOutput);
	    	hiveCatalogOutput.close();
		} catch (MetaException e) {
        	logger.log(Level.SEVERE, "Cannot access Hive Metastore ! Make sure the HiveMetastoreConfig.xml properties " +
        			"correctly references the hive-site.xml file location on your cluster.");
            System.exit(-100);
        }
	}
	
	/**
	 * Reads the hive-site configuration file present in the <i>hive_conf_home</i> folder specified in the HiveMetastoreConfig.xml.
	 * Adds the username and password to the configuration for accessing the Metastore catalog.
	 * @param hiveMetastoreProps a set of properties for accessing the HiveMetastore and exporting metadata
	 * @return a HiveConfiguration object for getting access to the HiveMetastore api.
	 */
	private static HiveConf getHiveConfiguration(Properties hiveMetastoreProps)
	{
		String hiveUser = hiveMetastoreProps.getProperty("metastore_user");
		String hiveConfFile = hiveMetastoreProps.getProperty("hive_conf_home") + 
				hiveMetastoreProps.getProperty("hive_conf_file");
		
		File hiveSiteFile = new File(hiveConfFile);
		if(!hiveSiteFile.exists()) {
			logger.log(Level.SEVERE, "Cannot find Hive Configuration file: " + hiveSiteFile);
			logger.log(Level.SEVERE, "Specify correct location in ./etc/HiveMetastoreConfig.xml");
			System.exit(-100);
		}
		
		HiveConf hiveConf = new HiveConf();
		Path hiveSite = new Path(hiveConfFile);
		
		logger.log(Level.INFO, "Reading hive properties from " + hiveSite);
		logger.log(Level.INFO, "Accessing Hive Metastore with user " + hiveUser);
		
		hiveConf.addResource(hiveSite);
        hiveConf.setVar(HiveConf.ConfVars.METASTORE_CONNECTION_USER_NAME, "id922010");
        
        return hiveConf;
	}

	private static void exportDatabases(HiveMetaStoreClient hiveClient,
			BufferedWriter hiveCatalogOutput) throws NoSuchObjectException, TException {
		List<String> databases = hiveClient.getAllDatabases();
		
		for(String dbName : databases) {
			DatabaseElement dbElement = new DatabaseElement(hiveClient.getDatabase(dbName));
			try {
				dbElement.writeRecord(hiveCatalogOutput);
			} catch (IOException e) {
				e.printStackTrace();
			}
			exportTables(hiveClient, hiveCatalogOutput, dbName);
		}
	}
	
	private static void exportTables(HiveMetaStoreClient hiveClient, BufferedWriter hiveCatalogOutput,
			String dbName) throws NoSuchObjectException, TException {
		List<String> tables = hiveClient.getAllTables(dbName);
		
		for(String tableName : tables) {
			TableElement tableElement = new TableElement(hiveClient.getTable(dbName, tableName));
			try {
				tableElement.writeRecord(hiveCatalogOutput);
			} catch (IOException e) {
				e.printStackTrace();
			}
			exportColumns(hiveClient, hiveCatalogOutput, dbName, tableName);
		}
	}
	
	private static void exportColumns(HiveMetaStoreClient hiveClient,
			BufferedWriter hiveCatalogOutput, String databaseName, String tableName) {
		
		try {
			Table table = hiveClient.getTable(databaseName, tableName);
			StorageDescriptor sd = table.getSd();
			for(FieldSchema field : sd.getCols()) {
				ColumnElement colElement = new ColumnElement(table, field);
				colElement.writeRecord(hiveCatalogOutput);
			}
		} catch (TException e) {
			e.printStackTrace();
		} catch (IOException e) {
			e.printStackTrace();
		}
	}
	
	private static void retrieveProperties(Properties hiveMetastoreProps, File propertiesFile)
	{
		logger.log(Level.INFO, "Reading HiveMetastoreReader properties from " + PROPERTIES_FILE);
		try {
			FileInputStream fin = new FileInputStream(propertiesFile);
			hiveMetastoreProps.loadFromXML(fin);
		} catch (IOException e) {
			logger.log(Level.SEVERE, "Properties file not found!");
		}
	}
}
