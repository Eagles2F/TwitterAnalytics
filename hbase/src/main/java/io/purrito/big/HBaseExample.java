package io.purrito.big;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

public class HBaseExample {
    public static int CONNECTION_POOL_SIZE = 10;
    private HTableInterface tweetTable;

    public HBaseExample(HTablePool pool)
    {
            tweetTable = pool.getTable("tweets");
    }

    public void close()
    {
        try

        {
            tweetTable.close();
        }
        catch (IOException e)
        {
            e.printStackTrace();
        }
    }

    public void put( Tweet tweet )
    {
        // Create a new Put object with the Row Key as the bytes of the user id
        Put put = new Put( Bytes.toBytes( tweet.getTweetId() ) );

        // Add the user id to the info column family
        put.add( Bytes.toBytes( "info" ),
                 Bytes.toBytes( "userId" ),
                 Bytes.toBytes( tweet.getUserId() ) );
        // Add the tweetId to the info column family
        put.add( Bytes.toBytes( "info" ),
                 Bytes.toBytes( "tweetId" ),
                 Bytes.toBytes( tweet.getTweetId() ) );
        // Add the time to the info column family
        put.add( Bytes.toBytes( "info" ),
                 Bytes.toBytes( "time" ),
                 Bytes.toBytes( tweet.getTime() ) );
        // Add the text to the info column family
        put.add( Bytes.toBytes( "info" ),
                 Bytes.toBytes( "text" ),
                 Bytes.toBytes( tweet.getText() ) );
        // Add the score to the info column family
        put.add( Bytes.toBytes( "info" ),
                 Bytes.toBytes( "score" ),
                 Bytes.toBytes( tweet.getScore() ) );

        try {
            // Add the tweet to the page view table
            tweetTable.put( put );
        }
        catch( IOException e )
        {
            e.printStackTrace();
        }
    }

    public Tweet get( String rowkey )

    {
        try
        {
            // Create a Get object with the rowkey (as a byte[])
            Get get = new Get( Bytes.toBytes( rowkey ) );

            // Execute the Get
            Result result = tweetTable.get( get );

            // Retrieve the results
            Tweet tweet = new Tweet();
            byte[] bytes = result.getValue( Bytes.toBytes( "info" ),
                                            Bytes.toBytes( "userId" ) );
            tweet.setUserId( Bytes.toString( bytes ) );
            bytes = result.getValue( Bytes.toBytes( "info" ),
                                     Bytes.toBytes( "tweetId" ) );
            tweet.setTweetId(Bytes.toString(bytes));
            bytes = result.getValue( Bytes.toBytes( "info" ),
                                     Bytes.toBytes( "text" ) );
            tweet.setText(Bytes.toString(bytes));
            bytes = result.getValue( Bytes.toBytes( "info" ),
                                     Bytes.toBytes( "time" ) );
            tweet.setTime(Bytes.toString(bytes));
            bytes = result.getValue( Bytes.toBytes( "info" ),
                                     Bytes.toBytes( "score" ) );
            tweet.setScore(Integer.valueOf(Bytes.toString(bytes)));

            // Return the newly constructed tweet
            return tweet;
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
            tweetTable.delete( delete );
        }
        catch (IOException e)
        {
            e.printStackTrace();
        }
    }

    public List<Tweet> scan( String startRowKey, String endRowKey )
    {
        try
        {
            // Build a list to hold our results
            List<Tweet> tweetResults = new ArrayList<Tweet>();

            // Create and execute a scan
            Scan scan = new Scan( Bytes.toBytes( startRowKey ), Bytes.toBytes( endRowKey ) );
            ResultScanner results = tweetTable.getScanner(scan);

            for( Result result : results )

            {
                // Build a new tweet
                Tweet tweet = new Tweet();
                byte[] bytes = result.getValue( Bytes.toBytes( "info" ),
                                                Bytes.toBytes( "userId" ) );
                tweet.setUserId( Bytes.toString( bytes ) );
                bytes = result.getValue( Bytes.toBytes( "info" ),
                                         Bytes.toBytes( "tweetId" ) );
                tweet.setTweetId(Bytes.toString(bytes));
                bytes = result.getValue( Bytes.toBytes( "info" ),
                                         Bytes.toBytes( "text" ) );
                tweet.setText(Bytes.toString(bytes));
                bytes = result.getValue( Bytes.toBytes( "info" ),
                                         Bytes.toBytes( "time" ) );
                tweet.setTime(Bytes.toString(bytes));
                bytes = result.getValue( Bytes.toBytes( "info" ),
                                         Bytes.toBytes( "score" ) );
                tweet.setScore(Integer.valueOf(Bytes.toString(bytes)));

                // Add the tweet to our results
                tweetResults.add( tweet );
            }

            // Return our results
            return tweetResults;
        }
        catch (IOException e)
        {
            e.printStackTrace();
        }
        return null;
    }

    public static void main( String[] args )

    {
        Configuration conf = HBaseConfiguration.create();
        conf.set("hbase.zookeeper.quorum", "#server’s IP address#");
        HTablePool pool = new HTablePool(conf, CONNECTION_POOL_SIZE);
        HBaseExample example = new HBaseExample(pool);

        // Create two records
        example.put( new Tweet( "User1", "/mypage", "asdfad", "asdfdsa", 13 ) );
        example.put( new Tweet( "User1", "/mypage", "asdfad", "asdfdsa", 132 ) );

        // Execute a Scan from "U" to "V"
        List<Tweet> tweets = example.scan( "U", "V" );
        if( tweets != null ) {
            System.out.println("Tweets:");
            for (Tweet tweet : tweets) {
                System.out.println("\tUser ID: " + tweet.getUserId() + ", TweetID: " + tweet.getTweetId());
            }
        }

        // Get a specific row
        Tweet tweet1 = example.get( "User1" );
        System.out.println("\tUser ID: " + tweet1.getUserId() + ", TweetID: " + tweet1.getTweetId());

        // Delete a row
        example.delete( "User1" );

        // Execute another scan, which should just have User2 in it
        tweets = example.scan( "U", "V" );
        if( tweets != null ) {
            System.out.println("Page Views:");
            for (Tweet tweet : tweets) {
                System.out.println("\tUser ID: " + tweet.getUserId() + ", TweetID: " + tweet.getTweetId());
            }
        }

        // Close our table
        example.close();
    }
}
