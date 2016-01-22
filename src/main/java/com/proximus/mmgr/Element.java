package com.proximus.mmgr;

import java.io.BufferedWriter;
import java.io.IOException;
import java.util.ArrayList;

/**
 * A metadata element part of an Informatica Metadata Manager custom model. A custom model is a collection of elements
 * organized in a hierarchy.<br>
 * For example, one might model a list of scripts, organized into folders, each scripts holding a list of tables which
 * themselves contains a list of columns<p>
 * <b>folder1 -> folder 2 -> script1 -> table1 -> column1</b><br>
 * Folder1, folder2, script1, table1 and colum1 are each individual <b>elements</b> of the model.<p>
 * An element represent an entry in a custom model. By default an element will contains following attributes:<p>
 * - <b>id</b>: a unique identifier of the element across the complete model<br>
 * - <b>name</b>: the name of element<br>
 * - <b>description</b>: an optional description of the element<br>
 * - <b>parentId</b>: the identifier of the parent's element in the model hierarchy. Empty if root element.<br>
 * - <b>type</b>: the type of element as specified in the model <i>load template</i>. Required by the Metadata Manager
 * load template when loading records from a csv file that contains multiple type of elements.<p>
 * 
 * Because the <b>id must be unique</b> across all elements in the model, it's frequently being generated as a concatenation of
 * the element's parent hierarchy and the element's name. Referring to the previous example, Script1 id would be equal to
 * <b>folder1.folder2.script1</b>.
 * 
 * @author Jonathan Puvilland
 */
public class Element implements ElementWritable {
	private char separator;
	private String id;
	private String name;
	private String description;
	private String parentId;
	private String type;
	
	private enum _HEADER {
		type,id,name,description,parent
	};
	
	public Element() {
		separator = Element.DEFAULT_SEPARATOR;
	}
	
	public Element(String id, String name, String type) {
		this();
		this.setId(id);
		this.setName(name);
		this.setDescription("");
		this.setParent("");
		this.setType(type);
	}
	
	public Element(String id, String name, String parentId, String type) {
		this(id, name, type);
		this.setDescription("");
		this.setParent(parentId);
	}
	
	public Element(String id, String name, String description, String parentId, String type) {
		this(id, name, parentId, type);
		this.setDescription(description);
	}

	/**
	 * @return the separator used for separate the Element's attribute.
	 */
	public char getSeprator() {
		return separator;
	}
	
	/**
	 * @param the separator used for separate the Element's attribute.
	 */
	public void setSeparator(char separator) {
		this.separator = separator;
	}

	/**
	 * @return the type of the element.
	 */
	public String getType() {
		return type;
	}

	/**
	 * @param type the type of element.
	 */
	public void setType(String type) {
		this.type = type;
	}

	/**
	 * @return the unique id of the element.
	 */
	public String getId() {
		return id;
	}

	/**
	 * @param id the unique id of the element.
	 */
	public void setId(String id) throws NullPointerException {
		if(id == null || id.isEmpty())
			throw new NullPointerException();
		this.id = id;
	}

	/**
	 * @return the name of the element.
	 */
	public String getName() {
		return name;
	}

	/**
	 * @param name the name of the element.
	 */
	void setName(String name) {
		if(name == null || name.isEmpty())
			throw new NullPointerException();
		this.name = name;
	}

	/**
	 * @return the description of the element.
	 */
	public String getDescription() {
		return description;
	}

	/**
	 * @param description the description of the element.
	 */
	public void setDescription(String description) {
		if(description == null)
			this.description = "";
		else
			this.description = description;
	}

	/**
	 * @return the parent id of the element.
	 */
	public String getParent() {
		return parentId;
	}

	/**
	 * @param parentId the parent identifier of the element.
	 */
	public void setParent(String parentId) {
		this.parentId = parentId;
	}
	
	/**
	 * Formats an array of strings as a single string separated with the specified separator. Replaces any null
	 * values with the empty string. Keeps the order of the array list.
	 * @param attributes an array list of string
	 * @param separator the separator between the strings
	 * @return a csv formated string
	 */
	public String formatRecord(ArrayList<String> attributes) {
		StringBuilder record = new StringBuilder();
		
		for(int i = 0; i < attributes.size(); i++) {
			String attribute = attributes.get(i);
			if(attribute == null)
				attribute = "";
			record.append(attribute + separator);
		}
		
		return record.substring(0, record.length() - 1);
	}
	
	/**
	 * @return a formated string of the Element attributes name separated by the object separator.
	 */
	public String getHeader() {
		StringBuilder header = new StringBuilder();
		for(_HEADER value : _HEADER.values()) {
			header.append(value);
			header.append(separator);
		}
		
		return	header.substring(0, header.length() -1);
	}
	
	/**
	 * @return a formated string of the Element values separated by the object separator.
	 */
	public String getRecord() {
		ArrayList<String> attributes = new ArrayList<String>();
		
		attributes.add(this.type);
		attributes.add(this.id);
		attributes.add(this.name);
		attributes.add(this.description);
		attributes.add(this.parentId);
		
		return formatRecord(attributes);
	}

	/**
	 * Writes the Element attribute values to the specified buffer
	 */
	@Override
	public void writeRecord(BufferedWriter buffer) throws IOException {		
		buffer.write(getRecord());
		buffer.newLine();
	}

	/**
	 * Writes the Element attribute names to the specified buffer
	 */
	@Override
	public void writeHeader(BufferedWriter buffer) throws IOException {
		buffer.write(getHeader());
		buffer.newLine();
	}
}
