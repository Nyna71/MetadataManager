package com.proximus.mmgr.hive.metastore;

import java.util.ArrayList;

import org.apache.hadoop.hive.metastore.api.StorageDescriptor;
import org.apache.hadoop.hive.metastore.api.Table;

import com.proximus.mmgr.Element;
import com.proximus.mmgr.ElementWritable;

/**
 * A sub-type of Elements that captures the Metadata of a Hive Metastore Table.
 * @author Jonathan Puvilland
 *
 */
public class TableElement extends Element implements ElementWritable {
	public static final String TABLE_ELEMENT_TYPE = "TBL";
	
	private Table table;
	
	/**
	 * The ordered list of a Table attributes
	 */
	private enum _HEADER {
		type,id,name,description,parent,tableType,locationUri,ownerName,
		viewOriginalText,viewExpandedText
	};

	/**
	 * Creates a TableElement initialized with a HiveMetastore Table Metadata
	 * @param table a HiveMetastore Table object
	 */
	public TableElement(Table table) {
		super(table.getDbName() + "." + table.getTableName(), table.getTableName(), "", 
				table.getDbName(), TABLE_ELEMENT_TYPE);
		
		// Get comment from table parameters if available
		if(table.getParametersSize() > 0)
			this.setDescription(table.getParameters().get("comment"));
		
		this.table = table;
	}
	
	@Override
	public String getHeader() {
		StringBuilder header = new StringBuilder();
		for(_HEADER value : _HEADER.values()) {
			header.append(value);
			header.append(this.getSeprator());
		}
		
		return	header.substring(0, header.length() -1);
	}

	@Override
	public String getRecord() {
		ArrayList<String> attributes = new ArrayList<String>();
		StorageDescriptor sd = table.getSd();
		
		attributes.add(super.getRecord());
		attributes.add(table.getTableType());
		attributes.add(sd.getLocation());
		attributes.add(table.getOwner());
		attributes.add(table.getViewOriginalText());
		attributes.add(table.getViewExpandedText());
		
		return super.formatAttributes(attributes);
	}

}
