package ir.assignments.three;

import java.util.Collection;

import edu.uci.ics.crawler4j.crawler.Page;
import edu.uci.ics.crawler4j.crawler.CrawlConfig;
import edu.uci.ics.crawler4j.crawler.CrawlController;
import edu.uci.ics.crawler4j.fetcher.PageFetcher;
import edu.uci.ics.crawler4j.robotstxt.RobotstxtConfig;
import edu.uci.ics.crawler4j.robotstxt.RobotstxtServer;

public class Crawler {
  /**
   * This method is for testing purposes only. It does not need to be used
   * to answer any of the questions in the assignment. However, it must
   * function as specified so that your crawler can be verified programatically.
   * 
   * This methods performs a crawl starting at the specified seed URL. Returns a
   * collection containing all URLs visited during the crawl.
   */
  public static Collection<String> crawl(String seedURL) {
    // TODO
    Page bs;
    
    return null;
  }

  public static void main(String[] args) throws Exception {

    CrawlConfig config = new CrawlConfig();
    
    config.setCrawlStorageFolder(Constants.CRAWL_STORAGE_FOLDER);
    config.setPolitenessDelay(Constants.POLITENESS);
    config.setMaxDepthOfCrawling(Constants.CRAWL_DEPTH);
    config.setSocketTimeout(Constants.SOCKET_TIMEOUT);
 //   config.setResumableCrawling(true);

    /*
     * Instantiate the controller for this crawl.
     */
    PageFetcher pageFetcher = new PageFetcher(config);
    RobotstxtConfig robotstxtConfig = new RobotstxtConfig();
    robotstxtConfig.setUserAgentName(Constants.USER_AGENT);
    RobotstxtServer robotstxtServer = new RobotstxtServer(robotstxtConfig, pageFetcher);
    CrawlController controller = new CrawlController(config, pageFetcher, robotstxtServer);

    /*
     * For each crawl, you need to add some seed urls. These are the first
     * URLs that are fetched and then the crawler starts following links
     * which are found in these pages
     */
    controller.addSeed("http://www.ics.uci.edu/");
    //          controller.addSeed("http://www.ics.uci.edu/~lopes/");
    //          controller.addSeed("http://mondego.ics.uci.edu/");

    /*
     * Start the crawl. This is a blocking operation, meaning that your code
     * will reach the line after this only when crawling is finished.
     */
    controller.start(BasicCrawler.class, Constants.NUM_CRALWERS);   
    if(controller.isFinished()) 
      System.out.println("Done !!");
  }
}