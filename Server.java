import org.vertx.java.core.Handler;
import org.vertx.java.core.http.HttpServerRequest;
import org.vertx.java.platform.Verticle;
import org.vertx.java.core.http.RouteMatcher;
import org.vertx.java.core.http.HttpServer;
import org.vertx.java.core.MultiMap;

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

import java.math.*;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.text.SimpleDateFormat;

public class Server extends Verticle {
  private final static String TEAM_ID = "purrito";
  private final static String AWS_ACCOUNT_ID = "3390-3551-2528";
  private final static String SECRET_KEY = "8271997208960872478735181815578166723519929177896558845922250595511921395049126920528021164569045773";

  public String decipher(String message, String key) {
    BigInteger big1 = new BigInteger(key);

    BigInteger big2 = new BigInteger(SECRET_KEY);

    BigInteger big3 = big1.divide(big2);

    long y = big3.longValue();

    int n = (int) Math.sqrt((double) message.length());
    StringBuilder sb = new StringBuilder();
    for (int i=0; i<2*n - 1; i++) {
        int z;
        if (i < n) {
            z = 0;
        } else {
            z = i - n + 1;
        }
        for (int j=z; j <= i - z; j++) {
            sb.append(message.charAt(j*n + i - j));
        }
    }

    String intermediate = sb.toString();
    int zz =(int) (y % 25 + 1);

    StringBuilder sb2 = new StringBuilder();
    for (int i = 0; i < intermediate.length(); i ++) {
        int order = intermediate.charAt(i) - 'A';
        if (order < zz) {
            sb2.append(Character.toChars('Z' - (zz - order - 1)));
        } else {
            sb2.append(Character.toChars(intermediate.charAt(i) - zz));
        }
    }
    return sb2.toString();
  }

  public void start() {
    final RouteMatcher router = new RouteMatcher();
    final HttpServer server = vertx.createHttpServer();
		server.setAcceptBacklog(32767);
		server.setUsePooledBuffers(true);
		server.setReceiveBufferSize(4 * 1024);
		server.setSendBufferSize(4 * 1024);

    router.get("/index.html", new Handler<HttpServerRequest>() {
			@Override
			public void handle(final HttpServerRequest req) {
        req.response().putHeader("Content-Type", "text/plain");
        req.response().putHeader("Content-Length", "0");
        req.response().end("");
			}
    });

    router.get("/q1", new Handler<HttpServerRequest>() {
			@Override
			public void handle(final HttpServerRequest req) {
				MultiMap map = req.params();
				final String key = map.get("key");
				final String message = map.get("message");
        String timestamp = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss").format(System.currentTimeMillis());
        String response = String.format("%s,%s\n%s\n%s\n", TEAM_ID,
          AWS_ACCOUNT_ID, timestamp, decipher(message, key));

        req.response().putHeader("Content-Type", "text/plain");
        req.response().putHeader("Content-Length",
          String.valueOf(response.length()));
        req.response().end(response);
			}
    });

    Configuration conf = HBaseConfiguration.create();
    conf.set("hbase.zookeeper.quorum", "52.91.198.252");
    conf.setInt("hbase.zookeeper.property.clientPort", 2181);
    try {
      final HConnection c = HConnectionManager.createConnection(conf);
      final HTableInterface table = c.getTable(Bytes.toBytes("tweet"));
      router.get("/q2", new Handler<HttpServerRequest>() {
  			@Override
  			public void handle(final HttpServerRequest req) {
  				MultiMap map = req.params();
  				final String userId = map.get("userid");
  				final String tweetTime = map.get("tweet_time");
				  // System.out.println(userId + " " + tweetTime);
          String response;

          String info = String.format("%s,%s\n", TEAM_ID, AWS_ACCOUNT_ID);
          StringBuilder sb = new StringBuilder();
          sb.append(info);
          Get g = new Get(Bytes.toBytes(userId+","+tweetTime));
          try {
            Result rr =table.get(g);
            String tweet = String.format("%s:%s:%s\n",
                Bytes.toString(rr.getValue(Bytes.toBytes("a"),Bytes.toBytes("id"))),
                Bytes.toString(rr.getValue(Bytes.toBytes("a"),Bytes.toBytes("score"))),
                Bytes.toString(rr.getValue(Bytes.toBytes("a"),Bytes.toBytes("text"))));
            sb.append(tweet);
            // System.out.println(tweet);
            response = sb.toString();

            response = response.replace("\\n","\n");
            // response = response.replace("\\a","\a");
            response = response.replace("\\b","\b");
            response = response.replace("\\f","\f");
            response = response.replace("\\r","\r");
            response = response.replace("\\t","\t");
            // response = response.replace("\\v","\v");
            response = response.replace("\\\'","\'");
            response = response.replace("\\\"","\"");
            response = response.replace("\\\\","\\");
            int length = 0;
            try {
                length = response.getBytes("utf-8").length;
            } catch (Exception e) {
                e.printStackTrace();
            }
            //System.out.println(response);

            req.response().putHeader("Content-Type", "text/plain;charset=utf-8");
            req.response().putHeader("Content-Length", String.valueOf(length));
            req.response().end(response, "utf-8");
          } catch (IOException e) {
                e.printStackTrace();
          }
        }
      });
    } catch (IOException e) {
        e.printStackTrace();
    }

    router.noMatch(new Handler<HttpServerRequest>() {
      @Override
      public void handle(final HttpServerRequest req) {
        req.response().putHeader("Content-Type", "text/plain");
        String response = "Not found.";
        req.response().putHeader("Content-Length",
          String.valueOf(response.length()));
        req.response().end(response);
        req.response().close();
      }
    });
    server.requestHandler(router);
    server.listen(80);
  }
}
