package com.proximus.mmgr;

import java.io.BufferedWriter;
import java.io.IOException;
import java.util.ArrayList;

/**
 * A metadata Element part of an Informatica Metadata Manager custom model. A custom model is a collection of Elements
 * organized in a hierarchy.<br>
 * For example, one might model a list of scripts, organized into folders, each scripts holding a list of tables which
 * themselves contains a list of columns<p>
 * <b>folder1 - folder 2 - script1 - table1 - column1</b><br>
 * Folder1, folder2, script1, table1 and colum1 are each individual <b>Elements</b> of the model.<p>
 * An Element represent an entry in a custom model. By default an Element will contains following attributes:<p>
 * - <b>id</b>: a unique identifier of the Element across the complete model<br>
 * - <b>name</b>: the name of Element<br>
 * - <b>description</b>: an optional description of the Element<br>
 * - <b>parentId</b>: the identifier of the parent's Element in the model hierarchy. Empty if root Element.<br>
 * - <b>type</b>: the type of Element as specified in the model <i>load template</i>. Required by the Metadata Manager
 * load template when loading records from a csv file that contains multiple type of Elements.<p>
 * 
 * Because the <b>id must be unique</b> across all Elements in the model, it's frequently being generated as a concatenation of
 * the Element's parent hierarchy and the Element's name. Referring to the previous example, Script1 id would be equal to
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
	
	/**
	 * Creates an empty Element
	 */
	public Element() {
		separator = Element.DEFAULT_SEPARATOR;
	}
	
	/**
	 * Creates a pre-polulated Element with an identifier, name and type
	 * @param id a unique identifier of the Element across the complete model
	 * @param name the name of Element
	 * @param type the type of Element as specified in the Informatica Metadata Manager model <i>load template</i>.
	 */
	public Element(String id, String name, String type) {
		this();
		this.setId(id);
		this.setName(name);
		this.setDescription("");
		this.setParent("");
		this.setType(type);
	}
	
	/**
	 * Creates a pre-polulated Element with an identifier, name, parent identifier and type
	 * @param id a unique identifier of the Element across the complete model
	 * @param name the name of Element
	 * @param parentId the identifier of the parent's Element in the model hierarchy 
	 * @param type the type of Element as specified in the Informatica Metadata Manager model <i>load template</i>.
	 */
	public Element(String id, String name, String parentId, String type) {
		this(id, name, type);
		this.setDescription("");
		this.setParent(parentId);
	}
	
	/**
	 * Creates a pre-polulated Element with an identifier, name, description, parent identifier and type
	 * @param id a unique identifier of the Element across the complete model
	 * @param name the name of Element
	 * @param description the description of the Element
	 * @param parentId the identifier of the parent's Element in the model hierarchy 
	 * @param type the type of Element as specified in the Informatica Metadata Manager model <i>load template</i>.
	 */
	public Element(String id, String name, String description, String parentId, String type) {
		this(id, name, parentId, type);
		this.setDescription(description);
	}

	/**
	 * Retrieves the Element's attributes separator
	 * @return the separator used to separate the Element's attribute.
	 */
	public char getSeprator() {
		return separator;
	}
	
	/**
	 * Sets the Element's attributes separator
	 * @param separator the separator used to separate the Element's attribute.
	 */
	public void setSeparator(char separator) {
		this.separator = separator;
	}

	/**
	 * Retrieves the type of Element part of a custom model
	 * @return the type of the Element.
	 */
	public String getType() {
		return type;
	}

	/**
	 * Sets the type of Element of a custom model
	 * @param type the type of the Element.
	 */
	public void setType(String type) {
		this.type = type;
	}

	/**
	 * Retrieves the unique identifier of the Element
	 * @return the unique identifier of the Element.
	 */
	public String getId() {
		return id;
	}

	/**
	 * Sets the unique identifier of the Element
	 * @param id the unique identifier of the Element.
	 * @throws NullPointerException if the provided identifier is empty or null
	 */
	public void setId(String id) throws NullPointerException {
		if(id == null || id.isEmpty())
			throw new NullPointerException();
		this.id = id;
	}

	/**
	 * Retrieves the name of the Element
	 * @return the name of the Element.
	 */
	public String getName() {
		return name;
	}

	/**
	 * Sets the name of the Element
	 * @param name the name of the Element.
	 * @throws NullPointerException if the provided name is empty or null
	 */
	void setName(String name) {
		if(name == null || name.isEmpty())
			throw new NullPointerException();
		this.name = name;
	}

	/**
	 * Retrieves the description of the Element
	 * @return the description of the Element.
	 */
	public String getDescription() {
		return description;
	}

	/**
	 * Sets the description of the Element
	 * @param description the description of the Element.
	 */
	public void setDescription(String description) {
		if(description == null)
			this.description = "";
		else
			this.description = description;
	}

	/**
	 * Retrieves the parent identifier of the Element
	 * @return the parent id of the Element.
	 */
	public String getParent() {
		return parentId;
	}

	/**
	 * Sets the parent identifier of the Element. This method does not control if the parent exists in the model.
	 * @param parentId the parent identifier of the Element.
	 */
	public void setParent(String parentId) {
		this.parentId = parentId;
	}
	
	/**
	 * Formats an array of strings as a single string separated with the Element separator. Replaces any null
	 * values with the empty string. Keeps the order of the array list.
	 * @param attributes an array list of string containing the Metadata attributes to format
	 * @return a csv formated string of the attributes
	 */
	public String formatAttributes(ArrayList<String> attributes) {
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
	 * Retrieves the Element's attributes header
	 * @return a formated string of the Element's attributes name separated by the object separator.
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
	 * Retrieves the Element's attributes value
	 * @return a formated string of the Element's attributes value separated by the object separator.
	 */
	public String getRecord() {
		ArrayList<String> attributes = new ArrayList<String>();
		
		attributes.add(this.type);
		attributes.add(this.id);
		attributes.add(this.name);
		attributes.add(this.description);
		attributes.add(this.parentId);
		
		return formatAttributes(attributes);
	}

	@Override
	public void writeRecord(BufferedWriter buffer) throws IOException {		
		buffer.write(getRecord());
		buffer.newLine();
	}

	@Override
	public void writeHeader(BufferedWriter buffer) throws IOException {
		buffer.write(getHeader());
		buffer.newLine();
	}
}
