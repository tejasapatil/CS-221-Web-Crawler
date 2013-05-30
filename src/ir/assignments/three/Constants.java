package ir.assignments.three;

import org.apache.cassandra.thrift.ConsistencyLevel;

/**
 * This class holds all the constants used for Cassandra client communication
 *  
 * @author Tejas Patil
 */
public class Constants {

  // String encoding used for storage in Cassandra
  public static final String UTF8 = "UTF8";

  // Ensure that the value is written to the commit log and memtable 
  // of at least one node before returning to the client.
  public static final ConsistencyLevel CL = ConsistencyLevel.ONE;

  // Cassandra server is being ran locally
  public static final String HOST = "localhost";

  // Default port used for connecting to the Cassandra server
  public static final int PORT = 9160;

  // Our keyspace for the webtable column family
  public static final String KEYSPACE = "demo";

  // Our keyspace for the webtable column family
  public static final String COLUMN_FAMILY = "webtable";

  // The location used by crawler4j for storing its data
  public static final String CRAWL_STORAGE_FOLDER = "/home/tejas/Desktop/CS221/data";

  // Politeness delay in milliseconds
  public static final int POLITENESS = 300;

  // Crawl Depth
  public static final int CRAWL_DEPTH = 3;

  // The timeout value for socket in milli-seconds
  public static final int SOCKET_TIMEOUT = 1000;
  
  // Number of crawler threads running
  public static final int NUM_CRALWERS = 5;
  
  // Number of crawler threads running
  public static final String USER_AGENT = "UCI IR crawler";

}
