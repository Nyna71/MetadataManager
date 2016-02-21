package com.proximus.mmgr;

import static org.junit.Assert.assertEquals;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;

import org.apache.commons.lang.NullArgumentException;
import org.junit.Test;

import com.proximus.mmgr.DefaultElementAttributes.DefaultAttributes;

public class SimpleElementTest {
	@Test
	public void valConstructors() {
		SimpleElement elem1 = new SimpleElement("id", "name", "parentId", "type");
		assertEquals(elem1.getAttribute(DefaultAttributes.id), "id");
		assertEquals(elem1.getAttribute(DefaultAttributes.name), "name");
		assertEquals(elem1.getAttribute(DefaultAttributes.parent), "parentId");

		SimpleElement elem2 = new SimpleElement("id", "name", "description", "parentId", "type");
		assertEquals(elem2.getAttribute(DefaultAttributes.id), "id");
		assertEquals(elem2.getAttribute(DefaultAttributes.name), "name");
		assertEquals(elem2.getAttribute(DefaultAttributes.description), "description");
		assertEquals(elem2.getAttribute(DefaultAttributes.parent), "parentId");
		assertEquals(elem2.getAttribute(DefaultAttributes.type), "type");
	}


	
	@Test(expected = NullArgumentException.class)
	public void valEmptyOrNullAttributes() {
		SimpleElement elem1 = new SimpleElement(null, null, null, null, null);
		elem1.setAttribute(DefaultAttributes.id, null);
		elem1.setAttribute(DefaultAttributes.id, "");
		elem1.setAttribute(DefaultAttributes.name, null);
		elem1.setAttribute(DefaultAttributes.name, "");
	}

	@Test
	public void valHeader() {
		SimpleElement elem1 = new SimpleElement("id", "name", "type");
		assertEquals(elem1.getHeader(), "type,id,name,description,parent");
	}

	@Test
	public void valRecords() {
		SimpleElement elem1 = new SimpleElement("id", "name", "description", "parent", "S");
		SimpleElement elem2 = new SimpleElement("id", "name", "S");
		assertEquals(elem1.getHeader(), "type,id,name,description,parent");
		assertEquals(elem1.getRecord(), "S,id,name,description,parent");
		assertEquals(elem2.getRecord(), "S,id,name,,");
	}

	@Test
	public void valExportFile() {
		SimpleElement elem1 = new SimpleElement("id", "name", "description", "parent", "S");
		SimpleElement elem2 = new SimpleElement("id", "name", "S");
		File file = new File("src/test/resources/elements.csv");
		try {
			BufferedWriter out = new BufferedWriter(new FileWriter(file));
			elem1.writeHeader(out);
			elem1.writeRecord(out);
			elem2.writeRecord(out);
			out.close();
		} catch (IOException e) {
			e.printStackTrace();
		} 
	}

	@Test
	public void valCustomSeparator() {
		SimpleElement elem1 = new SimpleElement("id", "name", "description", "parent", "S");
		elem1.setSeparator('|');
		assertEquals(elem1.getHeader(), "type|id|name|description|parent");
	}

	@Test
	public void valSeparatorInValue() {
		SimpleElement elem1 = new SimpleElement("id", "na,me", "desc,ription", "parent", "S");
		assertEquals(elem1.getRecord(), "S,id,\"na,me\",\"desc,ription\",parent");
	}
	
}
