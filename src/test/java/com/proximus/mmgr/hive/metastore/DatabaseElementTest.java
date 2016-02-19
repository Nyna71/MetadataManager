package com.proximus.mmgr.hive.metastore;

import static org.junit.Assert.assertEquals;
import com.proximus.mmgr.hive.metastore.DatabaseElement;
import org.apache.hadoop.hive.metastore.api.Database;

import org.junit.Test;

public class DatabaseElementTest {
	
	@Test
	public void valHeader() {
		Database db = new Database();
		db.setName("name");	
		DatabaseElement dbElem = new DatabaseElement(db);
		assertEquals(dbElem.getHeader(), "type,id,name,description,parent,locationUri,ownerName");
	}
	
	@Test
	public void valSeparator() {
		Database db = new Database();
		db.setName("name");
		DatabaseElement dbElem = new DatabaseElement(db);
		dbElem.setSeparator(';');
		assertEquals(dbElem.getHeader(), "type;id;name;description;parent;locationUri;ownerName");
	}

	@Test
	public void valRecord() {
		Database db = new Database();
		db.setName("name");
		db.setDescription("description");
		db.setLocationUri("locationUri");
		db.setOwnerName("ownerName");
		
		DatabaseElement dbElem = new DatabaseElement(db);
		System.out.println(dbElem.getRecord());
		assertEquals(dbElem.getRecord(), "DB,name,name,description,,locationUri,ownerName");
	}
	
	@Test
	public void valRecordNoDescription() {
		Database db = new Database();
		db.setName("name");
		db.setLocationUri("locationUri");
		db.setOwnerName("ownerName");

		DatabaseElement dbElem = new DatabaseElement(db);
		assertEquals(dbElem.getRecord(), "DB,name,name,,,locationUri,ownerName");
	}

}
