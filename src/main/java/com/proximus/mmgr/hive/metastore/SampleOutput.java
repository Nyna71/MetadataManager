package com.proximus.mmgr.hive.metastore;

import java.io.PrintWriter;

public class SampleOutput {

	public static void main(String[] args) throws Exception {
		PrintWriter hiveCatalogOutput = new PrintWriter("hive-catalogue.csv", "UTF-8");
		hiveCatalogOutput.println("Test");
		hiveCatalogOutput.close();
	}
}
