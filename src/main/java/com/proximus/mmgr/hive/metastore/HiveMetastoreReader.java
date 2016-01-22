package com.proximus.mmgr.hive.metastore;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.util.List;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.metastore.HiveMetaStoreClient;
import org.apache.hadoop.hive.metastore.api.MetaException;
import org.apache.hadoop.hive.metastore.api.NoSuchObjectException;
import org.apache.thrift.TException;

public class HiveMetastoreReader {
	private static final String HIVE_SITE = "/conf/hive-site.xml";

	public static void main(String[] args) throws Exception
	{
		if(!System.getenv().containsKey("HIVE_HOME"))
		{
			System.out.println("HIVE_HOME is missing. Please set HIVE_HOME folder location");
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
		System.out.println("hiveStite: " + hiveSite);
		
		hiveConf.addResource(hiveSite);
        hiveConf.setVar(HiveConf.ConfVars.METASTORE_CONNECTION_USER_NAME, "id922010");
        
        return hiveConf;
	}

	/**
	 * Exports Hive Metastore table's Metadata for a given Database. Following Table's Metadata is being exported:
	 * <br>- TABLE_NAME
	 * <br>- TABLE_OWNER
	 * <br>- TABLE_COMMENT
	 * <br>- TABLE_LOCATION
	 * <br>- TABLE_INPUT_FORMAT
	 * <br>- TABLE_OUTPOUT_FORMAT
	 * @param hiveClient A hive client connection
	 * @param hiveCatalogOutput The output FileWriter where table Metadata is exported to
	 * @param database The Hive Database for which Table's Metadata will be exported
	 * @throws NoSuchObjectException
	 * @throws TException
	 */
	private static void exportTables(HiveMetaStoreClient hiveClient, BufferedWriter hiveCatalogOutput,
			String database) throws NoSuchObjectException, TException {
		List<String> tables = hiveClient.getAllTables(database);
		
		for(String tbl : tables) {
			TableElement table = new TableElement(hiveClient.getTable(database, tbl));
			try {
				table.writeRecord(hiveCatalogOutput);
			} catch (IOException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		}
	}

	private static void exportDatabases(HiveMetaStoreClient hiveClient,
			BufferedWriter hiveCatalogOutput) throws NoSuchObjectException, TException {
		List<String> databases = hiveClient.getAllDatabases();
		
		for(String database : databases) {
			DatabaseElement db = new DatabaseElement(hiveClient.getDatabase(database));
			try {
				db.writeRecord(hiveCatalogOutput);
			} catch (IOException e) {
				e.printStackTrace();
			}
			exportTables(hiveClient, hiveCatalogOutput, database);
		}
	}
}
