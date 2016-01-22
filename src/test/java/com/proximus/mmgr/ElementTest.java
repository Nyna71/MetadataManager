package com.proximus.mmgr;

import static org.junit.Assert.assertEquals;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;

import org.junit.Test;

public class ElementTest {

	  @Test
	  public void valConstructors() {
	    Element elem1 = new Element("id", "name", "parentId", "type");
	    assertEquals(elem1.getId(), "id");
	    assertEquals(elem1.getName(), "name");
	    assertEquals(elem1.getParent(), "parentId");
	    assertEquals(elem1.getType(), "type");
	    
	    Element elem2 = new Element("id", "name", "description", "parentId", "type");
	    assertEquals(elem2.getId(), "id");
	    assertEquals(elem2.getName(), "name");
	    assertEquals(elem2.getDescription(), "description");
	    assertEquals(elem2.getParent(), "parentId");
	    assertEquals(elem2.getType(), "type");
	  }
	  
	  @Test(expected = NullPointerException.class)
	  public void valEmptyOrNullAttributes() {
		Element elem1 = new Element(null, null, null, null);
		elem1.setId(null);
		elem1.setId("");
		elem1.setName(null);
		elem1.setName("");
	  }
	  
	  @Test
	  public void valRecords() {
		  Element elem1 = new Element("id", "name", "description", "parent", "S");
		  Element elem2 = new Element("id", "name", "S");
		  System.out.println(elem1.getHeader());
		  assertEquals(elem1.getHeader(), "type,id,name,description,parent");
		  assertEquals(elem1.getRecord(), "S,id,name,description,parent");
		  assertEquals(elem2.getRecord(), "S,id,name,,");
	  }
	  
	  @Test
	  public void valExportFile() {
		  Element elem1 = new Element("id", "name", "description", "parent", "S");
		  Element elem2 = new Element("id", "name", "S");
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
}
