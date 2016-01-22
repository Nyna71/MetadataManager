package com.proximus.mmgr;

import java.io.BufferedWriter;
import java.io.IOException;

public interface ElementWritable {	
	public static final char DEFAULT_SEPARATOR = ',';
	
	public String getHeader();
	public String getRecord();
	
	public void writeRecord(BufferedWriter buffer) throws IOException;
	public void writeHeader(BufferedWriter buffer) throws IOException;
}
