package com.proximus.mmgr;

import java.io.BufferedWriter;
import java.io.IOException;

/**
 * An Interface that specifies methods for getting and writing Metadata Elements for Informatica Metadata Manager.
 * @author Jonathan
 *
 */
public interface ElementWritable {
	/**
	 * Default comma separator used to separate Elements attributes in the get methods
	 */
	public static final char DEFAULT_SEPARATOR = ',';
	public static final char HYPHEN = '"';
	public static final String HYPHEN_ESCAPE = "\"";
	
	/**
	 * Generates a record of the Element attributes name (header)
	 * @return a csv formatted string of Metadata Element's attributes names (header)
	 */
	public String getHeader();
	
	/**
	 * Generates a record of the Element attributes value
	 * @return a csv formatted string of Metadata Element's attributes value
	 */
	public String getRecord();
	
	/**
	 * Writes a a record of the Element's attributes name (header) to the specified file
	 * @param buffer the buffer wrapping the destination file
	 * @throws IOException in case of any IO failure when writing to the destination file
	 */
	public void writeHeader(BufferedWriter buffer) throws IOException;

	/**
	 * Writes a record of the Element's attributes value to the specified file
	 * @param buffer the buffer wrapping the destination file
	 * @throws IOException in case of any IO failure when writing to the destination file
	 */
	public void writeRecord(BufferedWriter buffer) throws IOException;
}
