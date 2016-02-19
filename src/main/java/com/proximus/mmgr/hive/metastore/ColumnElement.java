package com.proximus.mmgr.hive.metastore;

import org.apache.hadoop.hive.metastore.api.FieldSchema;
import org.apache.hadoop.hive.metastore.api.Table;

import com.proximus.mmgr.AbstractElement;
import com.proximus.mmgr.ElementWritable;
import com.proximus.mmgr.hive.metastore.ElementAttributes.ColumnAttributes;

/**
 * A sub-type of Elements that captures the Metadata of a Hive Metastore Column.
 * @author Jonathan Puvilland
 *
 */
public class ColumnElement extends AbstractElement<ColumnAttributes> implements ElementWritable {
	public static final String COL_ELEMENT_TYPE = "COL";
	
	/**
	 * Creates a ColumnElement initialized with a HiveMetastore Column (FieldSchema) Metadata
	 * @param table a HiveMetastore Table object
	 * @param col a HiveMetastore Column (FieldSchema) object
	 */
	public ColumnElement(Table table, FieldSchema col) {
		//super(table.getDbName() + "." + table.getTableName() + "." + col.getName(), col.getName(), col.getComment(), 
		//		table.getDbName() + "." + table.getTableName(), COL_ELEMENT_TYPE);

		super(ColumnAttributes.class);
		this.setAttribute(ColumnAttributes.id, table.getDbName() + "." + table.getTableName() + "." + col.getName());
		this.setAttribute(ColumnAttributes.name, col.getName());
		this.setAttribute(ColumnAttributes.description, col.getComment());
		this.setAttribute(ColumnAttributes.parent, table.getDbName() + "." + table.getTableName());
		this.setAttribute(ColumnAttributes.type, COL_ELEMENT_TYPE);
		this.setAttribute(ColumnAttributes.dataType, col.getType());
	}
}
