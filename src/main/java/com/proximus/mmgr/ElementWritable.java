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
	
	/**
	 * Generates a Metadata Element's attributes header (labels)
	 * @return a csv formatted string of Metadata Element's attributes header (labels)
	 */
	public String getHeader();
	
	/**
	 * Generates a Metadata Element's attributes value
	 * @return a csv formatted string of Metadata Element's attributes value
	 */
	public String getRecord();
	
	/**
	 * Writes a Metadata Element's attributes header (labels) to the specified file
	 * @param buffer the buffer wrapping the destination file
	 * @throws IOException in case of any IO failure when writing to the destination file
	 */
	public void writeHeader(BufferedWriter buffer) throws IOException;

	/**
	 * Writes a Metadata Element's attributes value to the specified file
	 * @param buffer the buffer wrapping the destination file
	 * @throws IOException in case of any IO failure when writing to the destination file
	 */
	public void writeRecord(BufferedWriter buffer) throws IOException;
}
