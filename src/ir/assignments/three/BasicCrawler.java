package ir.assignments.three;

import edu.uci.ics.crawler4j.crawler.Page;
import edu.uci.ics.crawler4j.crawler.WebCrawler;
import edu.uci.ics.crawler4j.url.WebURL;

import java.io.UnsupportedEncodingException;
import java.util.ArrayList;
import java.util.List;
import java.util.regex.Pattern;

import org.apache.cassandra.thrift.InvalidRequestException;
import org.apache.thrift.TException;
import org.apache.thrift.transport.TTransportException;

public class BasicCrawler extends WebCrawler {

  private final static Pattern FILTERS = Pattern.compile("^(https?)://(.*.)?ics.uci.edu/.*");
  private CassandraConnector connection;

  /** based on 
   * http://www.mkyong.com/regular-expressions/how-to-validate-image-file-extension-with-regular-expression/  */
  private static final Pattern IMAGE_PATTERN = Pattern.compile("([^\\s]+(\\.(?i)(" +
      "css|png|js|bmp|gif|jpeg?|tiff?|mid|mp2|mp3|mp4|wav|avi|mov|" +
      "mpeg|ram|m4v|pdf|rm|smil|wmv|swf|wma|zip|rar|gz))$)");

  @Override
  public void onStart() {
    try {
      connection = new CassandraConnector();
      connection.connect();
    } catch (UnsupportedEncodingException | TException
        | InvalidRequestException e) {
      e.printStackTrace();
    }
  }
  
  @Override
  protected void finalize() throws Throwable {  
    connection.close();
    super.finalize();
  }

  /**
   * Perform filtering to refrain from crawling image / video / exe etc urls 
   * Only crawl urls belonging to the ICS domain
   */
  @Override
  public boolean shouldVisit(WebURL url) {
    if (FILTERS.matcher(url.getURL()).matches())
      return !IMAGE_PATTERN.matcher(url.getURL()).matches();
    return false;
  }

  /**
   * This function is called when a page is fetched and ready to be processed.
   * Invokes the connector to insert the page to cassandra
   */
  @Override
  public void visit(Page page) {
    try {
      connection.insertUrl(page);
    } catch (Exception e) {
      System.out.println("Exception while inserting url : " + page.getWebURL().getURL());
      e.printStackTrace();
    }
  }

/*  public static void main(String [] args) {
    BasicCrawler bc = new BasicCrawler();
    WebURL url = new WebURL();

    List<String> urls = new ArrayList<String>();
    urls.add("http://www.tejas.ics.uci.edu/~lopes/teaching/cs221W13/assignments/");
    urls.add("http://www.gmail.com/~lopes/teaching/cs221W13/assignments/");
    urls.add("http://ics.uci.edu/~lopes/teaching/cs221W13/assignments/");
    urls.add("http://ics.uci.edu/dsds/lopes.PNG");

    for(int counter = 0; counter < urls.size(); counter++) {
      url.setURL(urls.get(counter));
      System.out.println("\nUrl = " + url.getURL() + "\nShould visit : " + bc.shouldVisit(url));
    }
  }*/
}