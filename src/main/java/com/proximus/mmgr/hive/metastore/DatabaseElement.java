package com.proximus.mmgr.hive.metastore;

import com.proximus.mmgr.AbstractElement;
import com.proximus.mmgr.ElementWritable;
import com.proximus.mmgr.hive.metastore.ElementAttributes.DatabasetAttributes;

import org.apache.hadoop.hive.metastore.api.Database;

/**
 * A sub-type of Elements that captures the Metadata of a HiveMetastore Database.
 * @author Jonathan Puvilland
 *
 */
public class DatabaseElement extends AbstractElement<DatabasetAttributes> implements ElementWritable {
	public static final String DATABASE_ELEMENT_TYPE = "DB";
	public static final String DATABASE_DEFAULT_PARENT = "";
	
	/**
	 * Creates a DatabaseElement initialized with a HiveMetastore Database Metadata.
	 * @param db a HiveMetastore Database object
	 */
	public DatabaseElement(Database db) {
		super(DatabasetAttributes.class);
		this.setAttribute(DatabasetAttributes.id, db.getName());
		this.setAttribute(DatabasetAttributes.name, db.getName());
		this.setAttribute(DatabasetAttributes.description, db.getDescription());
		this.setAttribute(DatabasetAttributes.parent, DATABASE_DEFAULT_PARENT);
		this.setAttribute(DatabasetAttributes.type, DATABASE_ELEMENT_TYPE);
		this.setAttribute(DatabasetAttributes.locationUri, db.getLocationUri());
		this.setAttribute(DatabasetAttributes.ownerName, db.getOwnerName());
	}
}
