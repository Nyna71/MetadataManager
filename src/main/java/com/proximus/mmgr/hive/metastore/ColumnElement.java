package com.proximus.mmgr.hive.metastore;

import java.util.ArrayList;

import org.apache.hadoop.hive.metastore.api.FieldSchema;
import org.apache.hadoop.hive.metastore.api.Table;

import com.proximus.mmgr.Element;
import com.proximus.mmgr.ElementWritable;

/**
 * A sub-type of Elements that captures the Metadata of a Hive Metastore Column.
 * @author Jonathan Puvilland
 *
 */
public class ColumnElement extends Element implements ElementWritable {
	public static final String COL_ELEMENT_TYPE = "COL";
	
	private FieldSchema col;
	
	/**
	 * The ordered list of a Column attributes
	 */
	private enum _HEADER {
		type,id,name,description,parent,colType
	};
	
	public ColumnElement(Table table, FieldSchema col) {
		super(table.getDbName() + "." + table.getTableName() + "." + col.getName(), col.getName(), col.getComment(), 
				table.getDbName() + "." + table.getTableName(), COL_ELEMENT_TYPE);

		this.col = col;
	}
	
	/**
	 * @return a formated string of the Column attributes name separated by the object separator.
	 */
	public String getHeader() {
		StringBuilder header = new StringBuilder();
		for(_HEADER value : _HEADER.values()) {
			header.append(value);
			header.append(this.getSeprator());
		}
		
		return	header.substring(0, header.length() -1);
	}
	
	/**
	 * @return a formated string of the Column values separated by the object separator.
	 */
	@Override
	public String getRecord() {
		ArrayList<String> attributes = new ArrayList<String>();
		
		attributes.add(super.getRecord());
		attributes.add(col.getType());
		
		return super.formatAttributes(attributes);
	}
}
