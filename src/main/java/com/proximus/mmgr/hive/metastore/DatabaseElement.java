package com.proximus.mmgr.hive.metastore;

import java.io.BufferedWriter;
import java.io.IOException;
import java.util.ArrayList;

import com.proximus.mmgr.Element;
import com.proximus.mmgr.ElementWritable;

import org.apache.hadoop.hive.metastore.api.Database;

/**
 * A sub-type of Elements that captures the Metadata of a Hive Metastore Database.
 * @author Jonathan Puvilland
 *
 */
public class DatabaseElement extends Element implements ElementWritable {
	public static final String DATABASE_ELEMENT_TYPE = "DB";
	public static final String DATABASE_DEFAULT_PARENT = "";
	
	private Database db;
	
	/**
	 * The ordered list of a Database attributes
	 */
	private enum _HEADER {
		type,id,name,description,parent,locationUri,ownerName
	};
	
	public DatabaseElement(Database db) {
		super(db.getName(), db.getName(), db.getDescription(), DATABASE_DEFAULT_PARENT, DATABASE_ELEMENT_TYPE);
		this.db = db;
	}
	
	/**
	 * @return a formated string of the Database attributes name separated by the specified separator.
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
	 * @return a formated string of the Element values separated by the specified separator.
	 */
	public String getRecord() {
		ArrayList<String> attributes = new ArrayList<String>();
		
		attributes.add(super.getRecord());
		attributes.add(db.getLocationUri());
		attributes.add(db.getOwnerName());
		
		return super.formatRecord(attributes);
	}

	@Override
	public void writeRecord(BufferedWriter buffer) throws IOException {
		buffer.write(this.getRecord());
		buffer.newLine();
	}

	@Override
	public void writeHeader(BufferedWriter buffer) throws IOException {
		buffer.write(this.getHeader());
		buffer.newLine();
	}

}
