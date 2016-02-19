package com.proximus.mmgr.hive.metastore;

import static org.junit.Assert.*;

import org.apache.hadoop.hive.metastore.api.StorageDescriptor;
import org.apache.hadoop.hive.metastore.api.Table;
import org.junit.Test;

import com.proximus.mmgr.hive.metastore.TableElement;

public class TableElementTest {

	@Test
	public void valHeader() {
		Table tbl = new Table();
		tbl.setTableName("name");
		tbl.setDbName("db");
		TableElement tblElem = new TableElement(tbl);
		assertEquals(tblElem.getHeader(), "type,id,name,description,parent,tableType,locationUri,ownerName," + 
				"viewOriginalText,viewExpandedText");
	}
	
	@Test
	public void valSeparator() {
		Table tbl = new Table();
		tbl.setTableName("name");
		TableElement tblElem = new TableElement(tbl);
		tblElem.setSeparator('|');
		assertEquals(tblElem.getHeader(), "type|id|name|description|parent|tableType|locationUri|ownerName|" + 
				"viewOriginalText|viewExpandedText");
	}
	
	@Test
	public void valTableRecord() {
		Table tbl = new Table();
		StorageDescriptor sd = new StorageDescriptor();
		
		sd.setLocation("locationUri");
		
		tbl.setDbName("db");
		tbl.setTableName("table");
		tbl.putToParameters("comment", "this is a table comment");
		tbl.setSd(sd);
		tbl.setOwner("ownerName");

		TableElement tblElem = new TableElement(tbl);
		assertEquals(tblElem.getRecord(), "TBL,db.table,table,this is a table comment,db,,locationUri,ownerName,,");
	}
	
	@Test
	public void valViewRecord() {
		Table tbl = new Table();
		StorageDescriptor sd = new StorageDescriptor();
		
		sd.setLocation("locationUri");
		
		tbl.setDbName("db");
		tbl.setTableName("view");
		tbl.putToParameters("comment", "this is a view comment");
		tbl.setSd(sd);
		tbl.setOwner("ownerName");
		tbl.setViewOriginalText("select * from test");
		tbl.setViewExpandedText("select col1, col2 from test");

		TableElement tblElem = new TableElement(tbl);
		assertEquals(tblElem.getRecord(), "TBL,db.view,view,this is a view comment,db,,locationUri,ownerName," +
				"select * from test,\"select col1, col2 from test\"");
	}

}
