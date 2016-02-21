package com.proximus.mmgr.hive.metastore;

/**
 * Interface for defining the list of Hive Metastore Element's attributes.
 * @author Jonathan Puvilland
 *
 */
interface ElementAttributes {
	static enum DatabasetAttributes { type, id, name, description, parent, locationUri, ownerName };
	static enum TableAttributes { type, id, name, description, parent, tableType, locationUri, ownerName, 
		viewOriginalText, viewExpandedText };
	static enum ColumnAttributes { type, id, name, description, parent, dataType };
}