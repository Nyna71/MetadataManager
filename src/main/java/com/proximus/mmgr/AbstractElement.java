package com.proximus.mmgr;

import java.io.BufferedWriter;
import java.io.IOException;
import java.util.EnumMap;
import java.util.Map;

import org.apache.commons.lang.NullArgumentException;

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
 * @param <AttributeType> An enumeation of the attributes applicable to the element
 */
public abstract class AbstractElement <AttributeType extends Enum<AttributeType>> implements ElementWritable {
	
	private char separator;
    private Map<AttributeType, String> attributes;
	
	/**
	 * Initializes the Map structure for storing the Element's attributes key-value pairs.
	 * The keys or attribute names must be listed in an enumeration type <i>AttributeType</i>.
	 * A default enumeration of attributes is available in the <i>DefaultElementAttributes</i> interface.
	 * @param attrType the enumeration class holding the attributes of the element
	 */
    public AbstractElement(Class<AttributeType> attrType) {
    	this.separator = DEFAULT_SEPARATOR;
        this.attributes = new EnumMap<AttributeType, String>(attrType);
    }

	/**
	 * Retrieves the Element's attributes separator.
	 * @return the separator used to separate the Element's attribute.
	 */
	public char getSeparator() {
		return separator;
	}
	
	/**
	 * Sets the Element's attributes separator.
	 * @param separator the separator used to separate the Element's attribute.
	 */
	public void setSeparator(char separator) {
		this.separator = separator;
	}
	
	/**
	 * Returns the attributes value specified by the attribute name.
	 * @param attrName the name of the attribute from the Element's type enumeration.
	 * @return the attribute value.
	 */
    public String getAttribute(AttributeType attrName) {
        return this.attributes.get(attrName);
    }
    
    /**
     * Sets the value of an attribute.
     * @param attrName the name of the attribute from the Element's type enumeration.
     * @param attrValue the value to set for the attribute.
     * @throws NullArgumentException <b>id</b> and <b>name</b> attributes are mandatory and cannot be set to null. Raises
     * an exception if attribute id or name is not defined properly.
     */
    public void setAttribute(AttributeType attrName, String attrValue) throws NullArgumentException{

    	if((attrName.toString() == "id" || attrName.toString() == "name") &&
    	   (attrValue == null || attrValue.isEmpty()))
				throw new NullArgumentException(attrName.toString());

		this.attributes.put(attrName, attrValue);
	}
    
    /**
     * Returns the list of all the attributes in the form of a key-value pair.
     * @return the list of attributes.
     */
    public Map<AttributeType, String> getAttributes() {
    	return this.attributes;
    }
	
	/**
	 * Retrieves the Element's attributes name.
	 * @return a formated string of the Element's attributes name separated by the Element separator.
	 */
	public String getHeader() {
		StringBuilder header = new StringBuilder();
		
		for(AttributeType attrName : attributes.keySet()) {
			header.append(attrName);
			header.append(separator);
		}
		
		return	header.substring(0, header.length() -1);
	}

	/**
	 * Retrieves the Element's attributes value. Replaces null values by empty string. Surrounds values with hyphen
	 * when value contains the Element separator.
	 * @return a formated string of the Element's attributes value separated by the Element separator.
	 */
	public String getRecord() {
		StringBuilder record = new StringBuilder();

		for(AttributeType attrName : attributes.keySet()) {
			String attribute = getAttribute(attrName);

			//replace null attributes with empty string
			if(attribute == null || attribute.equals(HYPHEN + "null" + HYPHEN))
				attribute = "";

			//adds hyphen escape character if separator is present in attribute's text
			else if(attribute.indexOf(separator) != -1)
				attribute = HYPHEN_ESCAPE + attribute + HYPHEN_ESCAPE;

			record.append(attribute + separator);
		}

		return record.substring(0, record.length() - 1);
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