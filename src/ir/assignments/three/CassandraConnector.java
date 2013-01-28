package ir.assignments.three;

import java.io.UnsupportedEncodingException;
import java.nio.ByteBuffer;

import org.apache.cassandra.thrift.Cassandra;
import org.apache.cassandra.thrift.Column;
import org.apache.cassandra.thrift.ColumnParent;
import org.apache.cassandra.thrift.InvalidRequestException;
import org.apache.log4j.Logger;
import org.apache.thrift.TException;
import org.apache.thrift.protocol.TBinaryProtocol;
import org.apache.thrift.protocol.TProtocol;
import org.apache.thrift.transport.TFramedTransport;
import org.apache.thrift.transport.TSocket;
import org.apache.thrift.transport.TTransport;
import org.apache.thrift.transport.TTransportException;

import edu.uci.ics.crawler4j.crawler.Page;

/**
 * This class creates a connection to the Cassandra server and 
 * handles all communication of the client to the Cassandra server
 * 
 * @author Tejas Patil
 */
public class CassandraConnector {

  private static final Logger LOG = Logger.getLogger(CassandraConnector.class);
  private TTransport tr = null;
  private Cassandra.Client client;
  private ColumnParent columnParent = null;
  private Column colDomain = null; 
  private Column colText= null; 
  private Column colPath = null; 
  private Column colParent = null; 
  private Column colsubDomain = null;  

  /**
   * This method returns a new connection to the Cassandra server 
   */
  public Cassandra.Client connect() 
      throws TTransportException,
      TException, UnsupportedEncodingException,
      InvalidRequestException {

    TTransport tr = new TSocket(Constants.HOST, Constants.PORT); 
    TFramedTransport tf = new TFramedTransport(tr); // Use the default, framed transport
    TProtocol proto = new TBinaryProtocol(tf);
    client = new Cassandra.Client(proto);
    tr.open();

    client.set_keyspace(Constants.KEYSPACE);                        // initialize it to our keyspace
    columnParent = new ColumnParent(Constants.COLUMN_FAMILY);       // and the column family
    colDomain = new Column(ByteBuffer.wrap("Domain".getBytes(Constants.UTF8)));
    colText = new Column(ByteBuffer.wrap("Text".getBytes(Constants.UTF8)));
    colPath = new Column(ByteBuffer.wrap("Path".getBytes(Constants.UTF8)));
    colParent = new Column(ByteBuffer.wrap("Parent".getBytes(Constants.UTF8)));
    colsubDomain = new Column(ByteBuffer.wrap("subDomain".getBytes(Constants.UTF8)));
    return client;
  }

  /**
   * Closes the connection to the Cassandra server 
   */
  public void close() {
    tr.close();
  }

  public void insertUrl(Page page) throws Exception {

    // This is the row key
    ByteBuffer url = ByteBuffer.wrap(page.getWebURL().getURL().getBytes(Constants.UTF8));   

    // create a representation of the Domain column
    colDomain.setValue(page.getWebURL().getDomain().getBytes(Constants.UTF8));
    colDomain.setTimestamp(System.currentTimeMillis());
    client.insert(url, columnParent, colDomain, Constants.CL);

    // create a representation of the Text column
    colText.setValue(page.getParseData().toString().getBytes());
    colText.setTimestamp(System.currentTimeMillis());
    client.insert(url, columnParent, colText, Constants.CL);

    // create a representation of the Path column
    colPath.setValue(page.getWebURL().getPath().getBytes(Constants.UTF8));
    colPath.setTimestamp(System.currentTimeMillis());
    client.insert(url, columnParent, colPath, Constants.CL);

    // create a representation of the Parent column
    try {
      colParent.setValue(page.getWebURL().getParentUrl().getBytes(Constants.UTF8));
      colParent.setTimestamp(System.currentTimeMillis());
      client.insert(url, columnParent, colParent, Constants.CL);
    } catch(NullPointerException e) {
      // This is possible when the page is root and doesnt have a parent
      // Can be safely ignored
    }

    // create a representation of the subDomain column
    colsubDomain.setValue(page.getWebURL().getSubDomain().getBytes(Constants.UTF8));
    colsubDomain.setTimestamp(System.currentTimeMillis());
    client.insert(url, columnParent, colsubDomain, Constants.CL);

    LOG.info("Insert performed for url: " + page.getWebURL().getURL());
  }

  public void readEntireDb(Page page) throws Exception {
    
 /*   SlicePredicate predicate = new SlicePredicate();
    
    SliceRange sliceRange = new SliceRange();
    sliceRange.setStart(new byte[0]);
    sliceRange.setFinish(new byte[0]);
    
    predicate.setSlice_range(sliceRange);
    
    List<ColumnOrSuperColumn> results = client.get_slice(key, parent, predicate, CL);
    
    for (ColumnOrSuperColumn cosc : results) { 
      Column c = cosc.column;
      System.out.println(new String(c.name, "UTF-8") + ": " + new String(c.value, "UTF-8"));
    }  
 */ }


  /*
    ColumnPath colPathName = new ColumnPath(COLUMN_FAMILY);
    colPathName.setColumn("Domain".getBytes(UTF8));

    colPathName = new ColumnPath(COLUMN_FAMILY);
    colPathName.setColumn("Text".getBytes(UTF8));

    colPathName = new ColumnPath(COLUMN_FAMILY);
    colPathName.setColumn("Path".getBytes(UTF8));

    colPathName = new ColumnPath(COLUMN_FAMILY);
    colPathName.setColumn("Parent".getBytes(UTF8));

    colPathName = new ColumnPath(COLUMN_FAMILY);
    colPathName.setColumn("subDomain".getBytes(UTF8));
   */
}