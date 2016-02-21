package com.proximus.mmgr.hive.metastore;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileWriter;
import java.io.IOException;
import java.security.InvalidParameterException;
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
 * The program support Kerberos authentication through keytab file or ticket. Authentication type is configurable
 * in a Java properties xml file. See readme file for more details.
 * @author Jonathan Puvilland
 *
 */
public class HiveMetastoreReader {
	private static final String KEYTAB_AUTHENTICATION_METHOD = "keytab";
	private static final String TICKET_AUTHENTICATION_METHOD = "ticket";
	private static final String NO_AUTHENTICATION_METHOD = "none";
	private static final String PROPERTIES_FILE = "./etc/HiveMetastoreConfig.xml";
	private static final Logger logger = Logger.getLogger(HiveMetastoreReader.class.getName());
	private static FileHandler logFileHandler;

	/**
	 * Reads the HiveMetastoreReader configuration file, connects to the HiveMetastore and exports Hive Metadata
	 * to a set of csv files 
	 * @param args not used
	 */
	public static void main(String[] args) 
	{
		HiveMetastoreReader thisReader = new HiveMetastoreReader();
		Properties metastoreReaderProperties = new Properties();
		
		//Read the program configuration file
		try {
			File metastoreReaderConfig = new File(PROPERTIES_FILE);
			metastoreReaderProperties = getMetastoreReaderProperties(metastoreReaderConfig);
			checkMetastoreReaderProperties(metastoreReaderProperties);
		} catch (IOException ioException) {
			StringBuilder errorMsg = new StringBuilder();
			errorMsg.append("Configuration file not found: " + PROPERTIES_FILE + "! ");
			errorMsg.append("Make sure the xml configuration file is avaialble in etc folder.");
			logger.log(Level.SEVERE, errorMsg.toString(), ioException);
			System.exit(-1);
		} catch (InvalidParameterException parameterException) {
			logger.log(Level.SEVERE, "Some mandatory properties are not set in the configuration file.", parameterException);
			System.exit(-1);
		}
		
		MetadataBufferedWriters bufferedWriters = thisReader.new MetadataBufferedWriters(metastoreReaderProperties);

		//Read hive-site configuration and creates a hive metastore client
		try {
			HiveMetaStoreClient hiveClient = new HiveMetaStoreClient(getHiveConfiguration(metastoreReaderProperties));
			exportheaders(bufferedWriters);
	    	exportDatabases(hiveClient, bufferedWriters);

		} catch (MetaException metaException) {
        	logger.log(Level.SEVERE, "Cannot access Hive Metastore ! Make sure the HiveMetastoreConfig.xml properties " +
        			"correctly references the hive-site.xml file location on your cluster.", metaException);
		} catch (InvalidParameterException parameterException) {
			logger.log(Level.SEVERE, parameterException.getMessage(), parameterException);
		} finally {
			bufferedWriters.closeBufferedWriters();
		}
	}
	
	/**
	 * Reads the HiveMetastoreReader configuration file in etc folder and creates a Properties object with
	 * runtime configuration parameters like Hive-Conf home folder, HiveMetastore credentials and output
	 * files location.
	 * @param readerConfiguration a HiveMetastoreReader xml configuration file
	 * @return HiveMetastoreReader runtime configuration parameters or null if configuration file is not found
	 * @throws IOException when configuration file cannot be read.
	 */
	static Properties getMetastoreReaderProperties (File readerConfiguration) throws IOException 
	{
		Properties metastoreReaderProperties = new Properties();
		
		logger.log(Level.INFO, "Reading HiveMetastoreReader properties from " + PROPERTIES_FILE);

		FileInputStream fin = new FileInputStream(readerConfiguration);
		metastoreReaderProperties.loadFromXML(fin);

		logFileHandler = new FileHandler(metastoreReaderProperties.getProperty("logFile", "default.log"));
		logFileHandler.setFormatter(new SimpleFormatter());
		logger.addHandler(logFileHandler);

		return metastoreReaderProperties;
	}

	/**
	 * Check for mandatory properties in the configuration file, namely:
	 * <br>- <b>hive_conf_home</b>: the folder location of hadoop hive configuration file.
	 * <br>- <b>hive_cong_file</b>: the name of the hive configuration file (ex. hive-site.xml).
	 * <br>- <b>authentication_method</b>: the hive metastore authentication method. Possible values are
	 * <i>none, ticket or keytab</i>.
	 * <br>If one of those properties is not defined, an exception is thrown and the program stops.
	 * @param hiveMetastoreProps
	 * @throws InvalidParameterException
	 */
	static void checkMetastoreReaderProperties(Properties hiveMetastoreProps) throws InvalidParameterException {
		if(!hiveMetastoreProps.containsKey("hive_conf_home"))
			throw new InvalidParameterException("Property hive_conf_home is not set!");
		
		if(!hiveMetastoreProps.containsKey("hive_conf_file"))
			throw new InvalidParameterException("Property hive_conf_file is not set!");
		
		if(!hiveMetastoreProps.containsKey("authentication_method"))
			throw new InvalidParameterException("Property authentication_method is not set!");
		
		String authenticationMethod = hiveMetastoreProps.getProperty("authentication_method");
		
		if(!authenticationMethod.equals(KEYTAB_AUTHENTICATION_METHOD) ||
				!authenticationMethod.equals(TICKET_AUTHENTICATION_METHOD) ||
				!authenticationMethod.equals(NO_AUTHENTICATION_METHOD))
			throw new InvalidParameterException("Invalid authentication_method: " + authenticationMethod);
	}
	
	/**
	 * Reads the hive-site configuration file present in the <i>hive_conf_home</i> folder specified in the HiveMetastoreConfig.xml.
	 * @param hiveMetastoreProps a set of properties for accessing the HiveMetastore and exporting metadata
	 * @return a HiveConfiguration object for getting access to the HiveMetastore api.
	 */
	static HiveConf getHiveConfiguration(Properties hiveMetastoreProps) throws InvalidParameterException
	{		
		String authenticationMethod= hiveMetastoreProps.getProperty("authentication_method");
		String hiveConfFile = hiveMetastoreProps.getProperty("hive_conf_home") + 
				hiveMetastoreProps.getProperty("hive_conf_file");
		
		// Reading hive-site.xml
		File hiveSiteFile = new File(hiveConfFile);
		if(!hiveSiteFile.exists()) {
			logger.log(Level.SEVERE, "Cannot find Hive Configuration file: " + hiveSiteFile);
			logger.log(Level.SEVERE, "Specify correct location in " + PROPERTIES_FILE);
			throw new InvalidParameterException("Invalid hive-site configuration file location!");
		}
		
		HiveConf hiveConf = new HiveConf();
		Path hiveSite = new Path(hiveConfFile);
		
		logger.log(Level.INFO, "Reading hive properties from " + hiveSite + "\n");
		
		hiveConf.addResource(hiveSite);
        
		// Set-Up Kerberos authentication
        hiveConf.set("hadoop.security.authentication", "Kerberos");
        UserGroupInformation.setConfiguration(hiveConf);
        
		try {
			// Keytab authentication
			if(authenticationMethod.equals(KEYTAB_AUTHENTICATION_METHOD)) {
				String keytab = hiveMetastoreProps.getProperty("kerberos_keytab");
				logger.log(Level.INFO, "Logging in HiveMetastore from Keytab " + keytab);
		        UserGroupInformation.loginUserFromKeytab("hive/sandbox.hortonworks.com@PROXIMUS.NET", keytab);
			}

			// Ticket authentication
			else if(authenticationMethod.equals(TICKET_AUTHENTICATION_METHOD)) {
				String ticket = hiveMetastoreProps.getProperty("kerberos_ticket");
				logger.log(Level.INFO, "Logging in HiveMetastore from ticket " + ticket);
				UserGroupInformation.createProxyUser("hive", 
						UserGroupInformation.getUGIFromTicketCache(ticket, ""));
			}
			
			else if(authenticationMethod.equals(NO_AUTHENTICATION_METHOD))
				logger.log(Level.INFO, "Logging in HiveMetastore without authentication");
			
			else {
				logger.log(Level.SEVERE, "Authentication method undefined. Set authentication_method in configuration file " + 
						"to keytab, ticket or none");
				return null;
			}
			
			logger.log(Level.INFO, "Connecting to Metastore with user: " + hiveConf.getUser());
			
		} catch (IOException ioException) {
			logger.log(Level.WARNING, "Cannot read Keytab file or Kerberos ticket.", ioException);
			return null;
		}
        
        return hiveConf;
	}
	
	/**
	 * Writes the Header records to the Database, Table and Column output files.
	 * @param bufferedWriters the object managing the different file writers.
	 */
	private static void exportheaders(MetadataBufferedWriters bufferedWriters) {
		DatabaseElement dbElement = new DatabaseElement();
		TableElement tableElement = new TableElement();
		ColumnElement colElement = new ColumnElement();
		
		try {
			dbElement.writeHeader(bufferedWriters.getDatabaseBufferedWriter());
		} catch (IOException ioException) {
			logger.log(Level.SEVERE, "Cannot write to database output file.", ioException);
		}
		
		try {
			tableElement.writeHeader(bufferedWriters.getTableBufferedWriter());
		} catch (IOException ioException) {
			logger.log(Level.SEVERE, "Cannot write to table output file.", ioException);
		}
		
		try {
			colElement.writeHeader(bufferedWriters.getColumnBufferedWriter());
		} catch (IOException ioException) {
			logger.log(Level.SEVERE, "Cannot write to column output file.", ioException);
		}
	}

	/**
	 * Writes the Hive Databases Metadata to the Databases output file.
	 * @param hiveClient the Hive Client session handler
	 * @param bufferedWriters the object managing the different file writers.
	 * @throws TException Thrift exception
	 */
	private static void exportDatabases(HiveMetaStoreClient hiveClient,
			MetadataBufferedWriters bufferedWriters)
	{
		try
		{
			List<String> databases = hiveClient.getAllDatabases();

			for(String dbName : databases)
			{
				logger.log(Level.INFO, "Exporting metadata for database: " + dbName);
				DatabaseElement dbElement = new DatabaseElement(hiveClient.getDatabase(dbName));
				dbElement.writeRecord(bufferedWriters.getDatabaseBufferedWriter());
				exportTables(hiveClient, bufferedWriters, dbName);
			}
			
		} catch (TException metaException) {
			logger.log(Level.SEVERE, "Cannot access HiveMetastore while processing Databases.", metaException);
		} catch (IOException ioException) {
			logger.log(Level.SEVERE, "Cannot write to database output file.", ioException);
		}
	}
	
	/**
	 * Writes the Hive Tables Metadata of the specified Database to the Tables output file.
	 * @param hiveClient the Hive Client session handler
	 * @param bufferedWriters the object managing the different file writers.
	 * @param dbName the name of the Database holding the Tables to export 
	 * @throws TException Thrift exception
	 */
	private static void exportTables(HiveMetaStoreClient hiveClient, MetadataBufferedWriters bufferedWriters,
			String dbName)
	{
		int nbrTablesExported = 0;
		
		try
		{	
			List<String> tables = hiveClient.getAllTables(dbName);			
		
			//Export tables Metadata
			for(String tableName : tables)
			{
				TableElement tableElement = new TableElement(hiveClient.getTable(dbName, tableName));
				tableElement.writeRecord(bufferedWriters.getTableBufferedWriter());
				exportColumns(hiveClient, bufferedWriters, dbName, tableName);
				nbrTablesExported++;
			}

			logger.log(Level.INFO, nbrTablesExported + " tables sucessfully exported.");
			
		} catch (TException metaException) {
			logger.log(Level.SEVERE, "Cannot access HiveMetastore while processing Tables.", metaException);
		} catch (IOException ioException) {
			logger.log(Level.SEVERE, "Cannot write to table output file.", ioException);
		}		
	}
	
	/**
	 * Writes the Hive Columns Metadata of the specified Database and Table to the Columns output file.
	 * @param hiveClient the Hive Client session handler
	 * @param bufferedWriters the object managing the different file writers.
	 * @param dbName the name of the Database holding the Table's Columns to export
	 * @param tableName the name of the Table holding the Columns to export 
	 */
	private static void exportColumns(HiveMetaStoreClient hiveClient,
			HiveMetastoreReader.MetadataBufferedWriters bufferedWriters, String dbName, String tableName)
	{	
		try
		{			
			Table table = hiveClient.getTable(dbName, tableName);
			StorageDescriptor sd = table.getSd();
			
			//Export columns Metadata
			for(FieldSchema field : sd.getCols())
			{
				ColumnElement colElement = new ColumnElement(table, field);
				colElement.writeRecord(bufferedWriters.getColumnBufferedWriter());
			}
		
		} catch (TException metaException) {
			logger.log(Level.SEVERE, "Cannot access HiveMetastore while processing Columns.", metaException);
		} catch (IOException ioException) {
			logger.log(Level.SEVERE, "Cannot write to column output file.", ioException);
		}
	}
	
	/**
	 * Helper class for opening and closing files to export Databases, Tables and Columns Metadata.
	 * @author Jonathan Puvilland
	 *
	 */
	 class MetadataBufferedWriters {
		private BufferedWriter databaseBuffer;
		private BufferedWriter tableBuffer;
		private BufferedWriter columnBuffer;
		Properties metastoreReaderProperties;
		
		
		/**
		 * The constructor opens the 3 destination files and handles the buffers.
		 * @param metastoreReaderProperties runtime configuration parameters containing specifications
		 * for the 3 output files.
		 */
		MetadataBufferedWriters(Properties metastoreReaderProperties) {
			
			this.metastoreReaderProperties = metastoreReaderProperties;
			
			databaseBuffer = openDatabaseBufferedWriter();
			tableBuffer = openTableBufferedWriter();
			columnBuffer = openColumnBufferedWriter();
			
		}

		private BufferedWriter openDatabaseBufferedWriter() {
			BufferedWriter databaseBuffer;
			
			try {
				File file = new File(
	        		metastoreReaderProperties.getProperty("metastore_output_dir") + "/" +
	        		metastoreReaderProperties.getProperty("metastore_database_file",
	        		"HiveMetastoreDatabases.csv"));
				
				logger.log(Level.INFO, "Opening database output file: " + file.getAbsolutePath());
				databaseBuffer = new BufferedWriter(new FileWriter(file));
				
				return databaseBuffer;
			
			} catch (IOException ioException) {
				logger.log(Level.SEVERE, "Cannot open database output file.", ioException);
				return null;
			}
		}

		private BufferedWriter openTableBufferedWriter() {
			BufferedWriter tableBuffer;
			
			try {
				File file = new File(
	        		metastoreReaderProperties.getProperty("metastore_output_dir") + "/" +
	        		metastoreReaderProperties.getProperty("metastore_table_file",
	        		"HiveMetastoreTables.csv"));
	        
				logger.log(Level.INFO, "Opening table output file: " + file.getAbsolutePath());
				tableBuffer = new BufferedWriter(new FileWriter(file));
				
				return tableBuffer;
			
			} catch (IOException ioException) {
				logger.log(Level.SEVERE, "Cannot open table output file.", ioException);
				return null;
			}
		}

		private BufferedWriter openColumnBufferedWriter() {
			BufferedWriter columnBuffer;

			try {
		        File file = new File(
		        		metastoreReaderProperties.getProperty("metastore_output_dir") + "/" +
		        		metastoreReaderProperties.getProperty("metastore_column_file",
		        		"HiveMetastoreColumns.csv"));
	        
				logger.log(Level.INFO, "Opening column output file: " + file.getAbsolutePath());
				columnBuffer = new BufferedWriter(new FileWriter(file));
				
				return columnBuffer;
			
			} catch (IOException ioException) {
				logger.log(Level.SEVERE, "Cannot open column output file.", ioException);
				return null;
			}
		}
		
		BufferedWriter getDatabaseBufferedWriter() {
			return databaseBuffer;
		}
		
		BufferedWriter getTableBufferedWriter() {
			return tableBuffer;
		}
		
		BufferedWriter getColumnBufferedWriter() {
			return columnBuffer;
		}
		
		void closeBufferedWriters() {
			try {
				databaseBuffer.close();
				tableBuffer.close();
				columnBuffer.close();
			} catch (IOException ioException) {
				logger.log(Level.SEVERE, "Cannot close buffered writers", ioException);
			}
		}
	}
}
