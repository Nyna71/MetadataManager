package com.proximus.mmgr;

import static org.junit.Assert.*;

import org.apache.hadoop.hive.metastore.api.FieldSchema;
import org.apache.hadoop.hive.metastore.api.Table;
import org.junit.Test;

import com.proximus.mmgr.hive.metastore.ColumnElement;

public class ColumnElementTest {

	@Test
	public void valHeader() {
		Table tbl = new Table();
		FieldSchema col = new FieldSchema();
		tbl.setTableName("name");
		col.setName("col");
		ColumnElement colElem = new ColumnElement(tbl, col);
		assertEquals(colElem.getHeader(), "type,id,name,description,parent,colType");
	}
	
	@Test
	public void valRecord() {
		Table tbl = new Table();
		FieldSchema col = new FieldSchema();
		tbl.setDbName("db");
		tbl.setTableName("table");
		col.setName("col");
		ColumnElement colElem = new ColumnElement(tbl, col);
		System.out.println(colElem.getRecord());
		assertEquals(colElem.getRecord(), "COL,db.table.col,col,,db.table,");
	}
	
	@Test
	public void valCommentAndType() {
		Table tbl = new Table();
		FieldSchema col = new FieldSchema();
		tbl.setDbName("db");
		tbl.setTableName("table");
		col.setName("col");
		col.setComment("comment");
		col.setType("type");
		ColumnElement colElem = new ColumnElement(tbl, col);
		assertEquals(colElem.getRecord(), "COL,db.table.col,col,comment,db.table,type");
	}

}
