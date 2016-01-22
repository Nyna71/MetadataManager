package com.proximus.mmgr;

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
	public void valRecordNoDescription() {
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

}
