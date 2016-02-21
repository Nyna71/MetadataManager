package com.proximus.mmgr.hive.metastore;

import org.apache.hadoop.hive.metastore.api.StorageDescriptor;
import org.apache.hadoop.hive.metastore.api.Table;

import com.proximus.mmgr.AbstractElement;
import com.proximus.mmgr.ElementWritable;
import com.proximus.mmgr.hive.metastore.ElementAttributes.TableAttributes;

/**
 * A sub-type of Elements that captures the Metadata of a HiveMetastore Table. The list of attributes is declared
 * in the <i>TableAttributes</i> interface. All the Element's manipulation methods are inherited from the
 * <i>AbstractElement</i> parent class.
 * @author Jonathan Puvilland
 *
 */
public class TableElement extends AbstractElement<TableAttributes> implements ElementWritable {
	public static final String TABLE_ELEMENT_TYPE = "TBL";
	
	public TableElement() {
		super(TableAttributes.class);
		this.setAttribute(TableAttributes.id, "id");
		this.setAttribute(TableAttributes.name, "name");
	}
	
	/**
	 * Creates a TableElement initialized with a HiveMetastore Table Metadata
	 * @param table a HiveMetastore Table object
	 */
	public TableElement(Table table) {
		super(TableAttributes.class);
		this.setAttribute(TableAttributes.id, table.getDbName() + "." + table.getTableName());
		this.setAttribute(TableAttributes.name, table.getTableName());
		this.setAttribute(TableAttributes.parent, table.getDbName());
		this.setAttribute(TableAttributes.type, TABLE_ELEMENT_TYPE);
		this.setAttribute(TableAttributes.tableType, table.getTableType());
		this.setAttribute(TableAttributes.ownerName, table.getOwner());
		this.setAttribute(TableAttributes.viewOriginalText, table.getViewOriginalText());
		this.setAttribute(TableAttributes.viewExpandedText, table.getViewExpandedText());
		
		// Table storage location is kept in a Storage Descriptor object
		StorageDescriptor sd = table.getSd();
		if(sd != null)
			this.setAttribute(TableAttributes.locationUri, sd.getLocation());
		else
			this.setAttribute(TableAttributes.locationUri, null);
		
		// Get comment from table parameters if available
		if(table.getParametersSize() > 0)
			this.setAttribute(TableAttributes.description, table.getParameters().get("comment"));
		else
			this.setAttribute(TableAttributes.description, null);
	}
}
