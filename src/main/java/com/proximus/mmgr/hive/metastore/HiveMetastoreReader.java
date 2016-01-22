package com.proximus.mmgr.hive.metastore;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.util.List;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.metastore.HiveMetaStoreClient;
import org.apache.hadoop.hive.metastore.api.FieldSchema;
import org.apache.hadoop.hive.metastore.api.MetaException;
import org.apache.hadoop.hive.metastore.api.NoSuchObjectException;
import org.apache.hadoop.hive.metastore.api.StorageDescriptor;
import org.apache.hadoop.hive.metastore.api.Table;
import org.apache.thrift.TException;

public class HiveMetastoreReader {
	private static final String HIVE_SITE = "/conf/hive-site.xml";

	public static void main(String[] args) throws Exception
	{
		if(!System.getenv().containsKey("HIVE_HOME"))
		{
			System.out.println("HIVE_HOME variable is required. Please set HIVE_HOME folder location.");
			System.exit(1);
		}
		System.out.println("Output file: " + args[0]);
		
		HiveMetaStoreClient hiveClient = new HiveMetaStoreClient(getHiveEnvironment());

        try
        {
        	File file = new File(args[0]);
			BufferedWriter hiveCatalogOutput = new BufferedWriter(new FileWriter(file));
			
    		exportDatabases(hiveClient, hiveCatalogOutput);
    		hiveCatalogOutput.close(); 
        } catch (MetaException e) {
            e.printStackTrace();
            System.err.println("Constructor error");
            System.err.println(e.toString());
            System.exit(-100);
        }
	}
	
	/**
	 * Reads the hive-site configuration file present in the HIVE_HOME folder specified as a OS environment variable.
	 * Adds the username and password to the configuration for accessing the Metastore catalog.
	 * @return a HiveConfiguration object
	 */
	private static HiveConf getHiveEnvironment()
	{
		HiveConf hiveConf = new HiveConf();
		String hiveHome = System.getenv("HIVE_HOME");
		Path hiveSite = new Path(hiveHome + HIVE_SITE);
		System.out.println("hiveSite: " + hiveSite);
		
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
}
