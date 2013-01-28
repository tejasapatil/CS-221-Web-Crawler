package ir.assignments.three;

import static ir.assignments.three.Constants.KEYSPACE;

import java.io.UnsupportedEncodingException;
import java.nio.ByteBuffer;

import org.apache.cassandra.thrift.Cassandra;
import org.apache.cassandra.thrift.Column;
import org.apache.cassandra.thrift.ColumnParent;
import org.apache.cassandra.thrift.ColumnPath;
import org.apache.cassandra.thrift.ConsistencyLevel;
import org.apache.cassandra.thrift.InvalidRequestException;
import org.apache.cassandra.thrift.NotFoundException;
import org.apache.cassandra.thrift.TimedOutException;
import org.apache.cassandra.thrift.UnavailableException;

import org.apache.log4j.Logger;
import org.apache.thrift.TException;
import org.apache.thrift.protocol.TBinaryProtocol;
import org.apache.thrift.protocol.TProtocol;
import org.apache.thrift.transport.TFramedTransport;
import org.apache.thrift.transport.TSocket;
import org.apache.thrift.transport.TTransport;

public class SimpleWriteRead {

  private static final Logger LOG = Logger.getLogger(SimpleWriteRead.class);
  //set up some constants 
  private static final String UTF8 = "UTF8";
  private static final String HOST = "localhost";
  private static final int PORT = 9160;
  private static final ConsistencyLevel CL = ConsistencyLevel.ONE;

  //not paying attention to exceptions here
  public static void main(String[] args) 
      throws UnsupportedEncodingException,
      InvalidRequestException, 
      UnavailableException, 
      TimedOutException,
      TException, 
      NotFoundException 
      {
    TTransport tr = new TSocket(HOST, PORT);

    //new default in 0.7 is framed transport
    TFramedTransport tf = new TFramedTransport(tr);
    TProtocol proto = new TBinaryProtocol(tf);
    Cassandra.Client client = new Cassandra.Client(proto);

    tf.open();
    client.set_keyspace(KEYSPACE);
    String cfName = "Standard1";
    ByteBuffer userIDKey = ByteBuffer.wrap("1".getBytes()); //this is a row key

    //create a representation of the Name column
    ColumnPath colPathName = new ColumnPath(cfName);
    colPathName.setColumn("name".getBytes(UTF8));
    ColumnParent cp = new ColumnParent(cfName);

    //insert the name column
    LOG.info("Inserting row for key 1");

    Column col1 = new Column(ByteBuffer.wrap("name".getBytes(UTF8)));
    col1.setValue("George Clinton".getBytes());
    col1.setTimestamp(System.currentTimeMillis());
    client.insert(userIDKey, cp, col1, CL);

    Column col2 = new Column(ByteBuffer.wrap("age".getBytes(UTF8)));
    col2.setValue("69".getBytes());    
    col2.setTimestamp(System.currentTimeMillis());
    client.insert(userIDKey, cp, col2, CL);

    LOG.info("Row insert done.");
    // read just the Name column
    LOG.info("Reading Name Column:");
    Column col = client.get(userIDKey, colPathName, CL).getColumn();
    LOG.info("Column name: " + new String(col.getName(), UTF8));
    LOG.info("Column value: " + new String(col.getValue(), UTF8));
    LOG.info("Column timestamp: " + col.getTimestamp());

    /*    //create a slice predicate representing the columns to read
    //start and finish are the range of columns--here, all
    SlicePredicate predicate = new SlicePredicate();
    SliceRange sliceRange = new SliceRange();
    sliceRange.setStart(new byte[0]);
    sliceRange.setFinish(new byte[0]);
    predicate.setSlice_range(sliceRange);
    LOG.debug("Complete Row:");
    // read all columns in the row
    ColumnParent parent = new ColumnParent(cfName);
    List<ColumnOrSuperColumn> results = 
        client.get_slice(userIDKey, 
            parent, predicate, CL);
    //loop over columns, outputting values
    for (ColumnOrSuperColumn result : results) {
      Column column = result.column;
      LOG.debug(new String(column.name, UTF8) + " : "
          + new String(column.value, UTF8));
    } */

    tf.close();
    LOG.debug("All done.");
   }
}