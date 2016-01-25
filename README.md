# MetadataManager
Exporting HiveMetastore Metadata to Informatica Metadata Manager

Contains a set of Classes to export Hive Metastore Metadata to a csv file
- Element: a class representing a base element of Informatica Metadata Manager model
- DatabaseElement: a sub-type of Element, representing a Hive Metastore database metadata
- TableElement: a sub-type of Element, representing a Hive Metastore table metadata
- ColumnElement: a sub-type of Element, representing a Hive Metastore column metadata

The MainClass (HiveMetastoreReader) reads the hive-site.xml configuration specified by the HIVE_HOME variable to get access to the Hive Metastore. It then uses various classes and methods from the org.apache.hadoop.hive.metastore.api to read and export metadadata to a csv file.
