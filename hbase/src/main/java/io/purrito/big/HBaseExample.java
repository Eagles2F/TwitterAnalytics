package io.purrito.big;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

public class HBaseExample
{
    private HTableInterface pageViewTable;

    public HBaseExample()
    {
        try
        {
            Configuration conf = HBaseConfiguration.create();
            pageViewTable = new HTable( conf, "PageViews");
        }
        catch (IOException e)
        {
            e.printStackTrace();
        }
    }

    public void close()
    {
        try

        {
            pageViewTable.close();
        }
        catch (IOException e)
        {
            e.printStackTrace();
        }
    }

    public void put( PageView pageView )
    {
        // Create a new Put object with the Row Key as the bytes of the user id
        Put put = new Put( Bytes.toBytes( pageView.getUserId() ) );

        // Add the user id to the info column family
        put.add( Bytes.toBytes( "info" ),
                 Bytes.toBytes( "userId" ),
                 Bytes.toBytes( pageView.getUserId() ) );

        // Add the page to the info column family
        put.add( Bytes.toBytes( "info" ),
                 Bytes.toBytes( "page" ),
                 Bytes.toBytes( pageView.getPage() ) );
        try

        {

            // Add the PageView to the page view table
            pageViewTable.put( put );
        }
        catch( IOException e )
        {
            e.printStackTrace();
        }
    }

    public PageView get( String rowkey )

    {
        try
        {

            // Create a Get object with the rowkey (as a byte[])
            Get get = new Get( Bytes.toBytes( rowkey ) );

            // Execute the Get
            Result result = pageViewTable.get( get );

            // Retrieve the results
            PageView pageView = new PageView();
            byte[] bytes = result.getValue( Bytes.toBytes( "info" ),
                                            Bytes.toBytes( "userId" ) );
            pageView.setUserId( Bytes.toString( bytes ) );
            bytes = result.getValue( Bytes.toBytes( "info" ),
                                     Bytes.toBytes( "page" ) );
            pageView.setPage(Bytes.toString(bytes));


            // Return the newly constructed PageView
            return pageView;
        }
        catch (IOException e)
        {
            e.printStackTrace();
        }
        return null;
    }
    public void delete( String rowkey )
    {
        try
        {
            Delete delete = new Delete( Bytes.toBytes( rowkey ) );
            pageViewTable.delete( delete );
        }
        catch (IOException e)
        {
            e.printStackTrace();
        }
    }

    public List<PageView> scan( String startRowKey, String endRowKey )
    {
        try
        {
            // Build a list to hold our results
            List<PageView> pageViewResults = new ArrayList<PageView>();


            // Create and execute a scan
            Scan scan = new Scan( Bytes.toBytes( startRowKey ), Bytes.toBytes( endRowKey ) );
            ResultScanner results = pageViewTable.getScanner(scan);

            for( Result result : results )

            {
                // Build a new PageView
                PageView pageView = new PageView();
                byte[] bytes = result.getValue( Bytes.toBytes( "info" ),
                        Bytes.toBytes( "userId" ) );
                pageView.setUserId( Bytes.toString( bytes ) );
                bytes = result.getValue( Bytes.toBytes( "info" ),
                        Bytes.toBytes( "page" ) );
                pageView.setPage(Bytes.toString(bytes));

                // Add the PageView to our results
                pageViewResults.add( pageView );
            }

            // Return our results
            return pageViewResults;
        }
        catch (IOException e)
        {
            e.printStackTrace();
        }
        return null;
    }

    public static void main( String[] args )

    {
        HBaseExample example = new HBaseExample();

        // Create two records
        example.put( new PageView( "User1", "/mypage" ) );
        example.put( new PageView( "User2","/mypage" ) );

        // Execute a Scan from "U" to "V"
        List<PageView> pageViews = example.scan( "U", "V" );
        if( pageViews != null ) {
            System.out.println("Page Views:");
            for (PageView pageView : pageViews) {
                System.out.println("\tUser ID: " + pageView.getUserId() + ", Page: " + pageView.getPage());
            }
        }

        // Get a specific row
        PageView pv = example.get( "User1" );
        System.out.println( "User ID: " + pv.getUserId() + ", Page: " + pv.getPage() );

        // Delete a row
        example.delete( "User1" );

        // Execute another scan, which should just have User2 in it
        pageViews = example.scan( "U", "V" );
        if( pageViews != null ) {
            System.out.println("Page Views:");
            for (PageView pageView : pageViews) {
                System.out.println("\tUser ID: " + pageView.getUserId() + ", Page: " + pageView.getPage());
            }
        }

        // Close our table
        example.close();
    }
}
