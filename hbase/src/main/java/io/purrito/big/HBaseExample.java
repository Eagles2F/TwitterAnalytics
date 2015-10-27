package io.purrito.big;

import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.client.HConnectionManager;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.client.HConnection;
import org.apache.hadoop.hbase.client.HTableInterface;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.hbase.filter.*;
import org.apache.hadoop.hbase.ZooKeeperConnectionException;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

public class HBaseExample {
    public static void main( String[] args )
    {
        Configuration conf = HBaseConfiguration.create();
        conf.set("hbase.zookeeper.quorum", "54.86.36.39");
        conf.setInt("hbase.zookeeper.property.clientPort", 2181);
        try {
        final HConnection c = HConnectionManager.createConnection(conf);
        Thread t = new Thread(new Runnable(){
            public void run() {
              try {
                  HTableInterface table = c.getTable(Bytes.toBytes("tweet"));

                  Get g = new Get(Bytes.toBytes("472550295504179200"));
                  Result r = table.get(g);
                  byte [] value = r.getValue(Bytes.toBytes("data"),
                    Bytes.toBytes("score"));

                  // If we convert the value bytes, we should get back 'Some Value', the
                  // value we inserted at this location.
                  String valueStr = Bytes.toString(value);
                  System.out.println("GET: " + valueStr);

                  Scan s = new Scan();
                  FilterList list = new FilterList(FilterList.Operator.MUST_PASS_ALL);
                  SingleColumnValueFilter userFilter = new SingleColumnValueFilter(
                      Bytes.toBytes("data"),
                      Bytes.toBytes("user_id"),
                      CompareFilter.CompareOp.EQUAL,
                      Bytes.toBytes("2262456624")
                  );
                  list.addFilter(userFilter);
                  SingleColumnValueFilter timeFilter = new SingleColumnValueFilter(
                      Bytes.toBytes("data"),
                      Bytes.toBytes("timestamp"),
                      CompareFilter.CompareOp.EQUAL,
                      Bytes.toBytes("2014-05-25 09:39:07")
                  );
                  list.addFilter(timeFilter);
                  s.setFilter(list);
                  s.setCaching(500);
                  ResultScanner scanner = table.getScanner(s);
                  try {
                      // Scanners return Result instances.
                      // Now, for the actual iteration. One way is to use a while loop like so:
                      for (Result rr = scanner.next(); rr != null; rr = scanner.next()) {
                        // print out the row we found and the columns we were looking for
                        System.out.println("tweet Id: " + Bytes.toString(rr.getRow()));
                          System.out.println("Tweet Id: " + Bytes.toString(rr.getValue(Bytes.toBytes("data"),
                            Bytes.toBytes("text"))));
                            System.out.println("Score: " + Bytes.toString(rr.getValue(Bytes.toBytes("data"),
                              Bytes.toBytes("score"))));
                              System.out.println("Timestamp: " + Bytes.toString(rr.getValue(Bytes.toBytes("data"),
                                Bytes.toBytes("timestamp"))));
                                System.out.println("User Id: " + Bytes.toString(rr.getValue(Bytes.toBytes("data"),
                                  Bytes.toBytes("user_id"))));
                      }

                      // The other approach is to use a foreach loop. Scanners are iterable!
                      // for (Result rr : scanner) {
                      //   System.out.println("Found row: " + rr);
                      // }
                  } finally {
                    // Make sure you close your scanners when you are done!
                    // Thats why we have it inside a try/finally clause
                    scanner.close();
                  }
                } catch (IOException e) {
                    e.printStackTrace();
                }
            }
        });
        t.start();
      } catch (ZooKeeperConnectionException e) {
          e.printStackTrace();
      }
    }
}
