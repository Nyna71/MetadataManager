package com.proximus.mmgr;

import com.proximus.mmgr.DefaultElementAttributes.DefaultAttributes;

/**
 * A class that defines a simple Metadata Element, with a few attributes. The list of attributes is declared
 * in the <i>DefaultAttributes</i> interface. All the Element's manipulation methods are inherited from the
 * <i>AbstractElement</i> parent class.
 * Offers various constructors for creating a simple Element.
 * @author Jonathan Puvilland
 *
 */
public class SimpleElement extends AbstractElement<DefaultAttributes> {

	public SimpleElement(String id, String name) {
		super(DefaultAttributes.class);
		this.setAttribute(DefaultAttributes.id, id);
		this.setAttribute(DefaultAttributes.name, name);
		this.setAttribute(DefaultAttributes.description, null);
		this.setAttribute(DefaultAttributes.parent, null);
		this.setAttribute(DefaultAttributes.type, null);
	}
	
	public SimpleElement(String id, String name, String type) {
		this(id, name);
		this.setAttribute(DefaultAttributes.type, type);
	}
	
	public SimpleElement(String id, String name, String parent, String type) {
		this(id, name, type);
		this.setAttribute(DefaultAttributes.parent, parent);
	}
	
	public SimpleElement(String id, String name, String description, String parent, String type) {
		this(id, name, parent, type);
		this.setAttribute(DefaultAttributes.description, description);
	}
}
