package com.proximus.mmgr.hive.metastore;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileWriter;
import java.io.IOException;
import java.util.List;
import java.util.Properties;
import java.util.logging.FileHandler;
import java.util.logging.Level;
import java.util.logging.Logger;
import java.util.logging.SimpleFormatter;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.metastore.HiveMetaStoreClient;
import org.apache.hadoop.hive.metastore.api.FieldSchema;
import org.apache.hadoop.hive.metastore.api.MetaException;
import org.apache.hadoop.hive.metastore.api.StorageDescriptor;
import org.apache.hadoop.hive.metastore.api.Table;
import org.apache.thrift.TException;
import org.apache.hadoop.security.UserGroupInformation;

/**
 * Reads the Hive Metastore repository using the Hive Metastore api, and exports Hive Metadata into a set of
 * csv files ready to be uploaded in Informatica Metadata Manager.
 * @author Jonathan Puvilland
 *
 */
public class HiveMetastoreReader {
	private static final String KEYTAB_AUTHENTICATION_METHOD = "keytab";
	private static final String TICKET_AUTHENTICATION_METHOD = "ticket";
	private static final String PROPERTIES_FILE = "./etc/HiveMetastoreConfig.xml";
	private static final Logger logger = Logger.getLogger(HiveMetastoreReader.class.getName());
	private static FileHandler logFileHandler;

	/**
	 * Reads the HiveMetastoreReader configuration file, connects to the HiveMetastore and exports Hive Metadata
	 * to a set of csv files 
	 * @param args not used
	 * @throws Exception
	 */
	public static void main(String[] args) 
	{
		Properties metastoreReaderProperties = new Properties();
		File metastoreReaderConfig = new File(PROPERTIES_FILE);
		
		metastoreReaderProperties = getMetastoreReaderProperties(metastoreReaderConfig);
		
		try {
			HiveMetaStoreClient hiveClient = new HiveMetaStoreClient(getHiveConfiguration(metastoreReaderProperties));

	        File file = new File(metastoreReaderProperties.getProperty("metastore_output_file"));
			BufferedWriter hiveCatalogOutput = new BufferedWriter(new FileWriter(file));
	    	exportDatabases(hiveClient, hiveCatalogOutput);
	    	hiveCatalogOutput.close();
		} catch (MetaException e) {
        	logger.log(Level.SEVERE, "Cannot access Hive Metastore ! Make sure the HiveMetastoreConfig.xml properties " +
        			"correctly references the hive-site.xml file location on your cluster.");
            System.exit(-100);
        } catch (IOException e) {
        	logger.log(Level.SEVERE, "Cannot open Metastore output file");
			e.printStackTrace();
		} 
	}
	
	/**
	 * Reads the HiveMetastoreReader configuration file in etc folder and creates a Properties object with
	 * runtime configuration parameters like Hive-Conf home folder, HiveMetastore credentials and output
	 * files location.
	 * @param readerConfiguration a HiveMetastoreReader xml configuration file
	 * @return HiveMetastoreReader runtime configuration parameters
	 */
	private static Properties getMetastoreReaderProperties (File readerConfiguration)
	{
		Properties metastoreReaderProperties = new Properties();
		
		logger.log(Level.INFO, "Reading HiveMetastoreReader properties from " + PROPERTIES_FILE);
		try {
			FileInputStream fin = new FileInputStream(readerConfiguration);
			metastoreReaderProperties.loadFromXML(fin);
			
			logFileHandler = new FileHandler(metastoreReaderProperties.getProperty("logFile", "default.log"));
			logFileHandler.setFormatter(new SimpleFormatter());
			logger.addHandler(logFileHandler);
			
		} catch (IOException e) {
			StringBuilder errorMsg = new StringBuilder();
			errorMsg.append("Configuration file not found: " + PROPERTIES_FILE + "! ");
			errorMsg.append("Make sure the xml configuration file is avaialble in etc folder.");
			logger.log(Level.SEVERE, errorMsg.toString());
			System.exit(-100);
		}

		return metastoreReaderProperties;
	}
	
	/**
	 * Reads the hive-site configuration file present in the <i>hive_conf_home</i> folder specified in the HiveMetastoreConfig.xml.
	 * @param hiveMetastoreProps a set of properties for accessing the HiveMetastore and exporting metadata
	 * @return a HiveConfiguration object for getting access to the HiveMetastore api.
	 */
	private static HiveConf getHiveConfiguration(Properties hiveMetastoreProps)
	{
		String hiveUser = hiveMetastoreProps.getProperty("metastore_user");
		String authenticationMethod= hiveMetastoreProps.getProperty("authentication_method");
		String hiveConfFile = hiveMetastoreProps.getProperty("hive_conf_home") + 
				hiveMetastoreProps.getProperty("hive_conf_file");
		
		// Reading hive-site.xml
		File hiveSiteFile = new File(hiveConfFile);
		if(!hiveSiteFile.exists()) {
			logger.log(Level.SEVERE, "Cannot find Hive Configuration file: " + hiveSiteFile);
			logger.log(Level.SEVERE, "Specify correct location in " + PROPERTIES_FILE);
			System.exit(-100);
		}
		
		HiveConf hiveConf = new HiveConf();
		Path hiveSite = new Path(hiveConfFile);
		
		logger.log(Level.INFO, "Reading hive properties from " + hiveSite + "\n");
		
		hiveConf.addResource(hiveSite);
        //hiveConf.setVar(HiveConf.ConfVars.METASTORE_CONNECTION_USER_NAME, hiveUser);
        //hiveConf.setVar(HiveConf.ConfVars.METASTORE_KERBEROS_PRINCIPAL, "hive/sandbox.hortonworks.com@PROXIMUS.NET");
        
		// Set-Up Kerberos authentication
        hiveConf.set("hadoop.security.authentication", "Kerberos");
        UserGroupInformation.setConfiguration(hiveConf);
        
		try {
			if(authenticationMethod.equals(KEYTAB_AUTHENTICATION_METHOD)) {
				String keytab = hiveMetastoreProps.getProperty("kerberos_keytab");
				logger.log(Level.INFO, "Logging in HiveMetastore from Keytab " + keytab);
		        UserGroupInformation.loginUserFromKeytab("hive/sandbox.hortonworks.com@PROXIMUS.NET", keytab);
			}

			else if(authenticationMethod.equals(TICKET_AUTHENTICATION_METHOD)) {
				String ticket = hiveMetastoreProps.getProperty("kerberos_ticket");
				logger.log(Level.INFO, "Logging in HiveMetastore from ticket " + ticket);
				UserGroupInformation ugi = UserGroupInformation.createProxyUser("hive", 
						UserGroupInformation.getUGIFromTicketCache(ticket, "hive"));
			}
			
			else {
				logger.log(Level.SEVERE, "Authentication method undefined. Set authentication_method in configuration file " + 
						"to kerberos_keytab or kerberos_ticket");
			}
			
			System.out.println("Connecting with user: " + hiveConf.getUser());
			
		} catch (IOException e) {
			logger.log(Level.WARNING, "Cannot read Keytab file or Kerberos ticket.");
		}
        
        return hiveConf;
	}

	/**
	 * Writes the Hive Databases Metadata to the Databases output file.
	 * @param hiveClient the Hive Client session handler
	 * @param hiveCatalogOutput the buffer wrapping the output database file
	 * @throws TException Thrift exception
	 */
	private static void exportDatabases(HiveMetaStoreClient hiveClient,
			BufferedWriter hiveCatalogOutput)
	{
		try
		{
			List<String> databases = hiveClient.getAllDatabases();
		
			for(String dbName : databases)
			{
				DatabaseElement dbElement = new DatabaseElement(hiveClient.getDatabase(dbName));
				dbElement.writeRecord(hiveCatalogOutput);
				exportTables(hiveClient, hiveCatalogOutput, dbName);
			}
			
		} catch (TException e) {
			logger.log(Level.SEVERE, "Cannot access HiveMetastore while processing Columns.");
			e.printStackTrace();
		
		} catch (IOException e) {
			logger.log(Level.SEVERE, "Cannot write to Table output file.");
			e.printStackTrace();
		}
	}
	
	/**
	 * Writes the Hive Tables Metadata of the specified Database to the Tables output file.
	 * @param hiveClient the Hive Client session handler
	 * @param hiveCatalogOutput the buffer wrapping the output table file
	 * @param dbName the name of the Database holding the Tables to export 
	 * @throws TException Thrift exception
	 */
	private static void exportTables(HiveMetaStoreClient hiveClient, BufferedWriter hiveCatalogOutput,
			String dbName)
	{
		try
		{
			List<String> tables = hiveClient.getAllTables(dbName);
		
			for(String tableName : tables)
			{
				TableElement tableElement = new TableElement(hiveClient.getTable(dbName, tableName));
				tableElement.writeRecord(hiveCatalogOutput);
				exportColumns(hiveClient, hiveCatalogOutput, dbName, tableName);
			}
			
		} catch (TException e) {
			logger.log(Level.SEVERE, "Cannot access HiveMetastore while processing Columns.");
			e.printStackTrace();
		
		} catch (IOException e) {
			logger.log(Level.SEVERE, "Cannot write to Table output file.");
			e.printStackTrace();
		}
	}
	
	/**
	 * Writes the Hive Columns Metadata of the specified Database and Table to the Columns output file.
	 * @param hiveClient the Hive Client session handler
	 * @param hiveCatalogOutput the buffer wrapping the output table file
	 * @param dbName the name of the Database holding the Table's Columns to export
	 * @param tableName the name of the Table holding the Columns to export 
	 */
	private static void exportColumns(HiveMetaStoreClient hiveClient,
			BufferedWriter hiveCatalogOutput, String dbName, String tableName)
	{	
		try
		{
			Table table = hiveClient.getTable(dbName, tableName);
			StorageDescriptor sd = table.getSd();
			
			for(FieldSchema field : sd.getCols())
			{
				ColumnElement colElement = new ColumnElement(table, field);
				colElement.writeRecord(hiveCatalogOutput);
			}
		
		} catch (TException e) {
			logger.log(Level.SEVERE, "Cannot access HiveMetastore while processing Columns.");
			e.printStackTrace();
		
		} catch (IOException e) {
			logger.log(Level.SEVERE, "Cannot write to Table output file.");
			e.printStackTrace();
		}
	}
}
