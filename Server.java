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
import java.util.Date;
import java.util.Comparator;
import java.util.Collections;

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
    conf.set("hbase.zookeeper.quorum", "54.164.84.203");
    conf.setInt("hbase.zookeeper.property.clientPort", 2181);
    try {
      final HConnection c = HConnectionManager.createConnection(conf);
      final HTableInterface table = c.getTable(Bytes.toBytes("tweet"));
      router.get("/q2", new Handler<HttpServerRequest>() {
  			@Override
  			public void handle(final HttpServerRequest req) {
          // final long req_start = System.currentTimeMillis();
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
            final long start_time = System.currentTimeMillis();
            // System.out.println("mills taken before backend:" + (start_time - req_start));
            Result rr =table.get(g);
            String tweet = String.format("%s:%s:%s\n",
                Bytes.toString(rr.getValue(Bytes.toBytes("a"),Bytes.toBytes("id"))),
                Bytes.toString(rr.getValue(Bytes.toBytes("a"),Bytes.toBytes("score"))),
                Bytes.toString(rr.getValue(Bytes.toBytes("a"),Bytes.toBytes("text"))));
            sb.append(tweet);
            final long end_time = System.currentTimeMillis();
            System.out.println("mills taken for backend:" + (end_time - start_time));

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
            // final long req_end = System.currentTimeMillis();
            // System.out.println("mills taken after backend:" + (req_end - end_time));
          } catch (IOException e) {
                e.printStackTrace();
          }
        }
      });

      router.get("/q3", new Handler<HttpServerRequest>() {
  			@Override
  			public void handle(final HttpServerRequest req) {
          // final long req_start = System.currentTimeMillis();
  				MultiMap map = req.params();
  				final String start_date = map.get("start_date");
  				final String end_date = map.get("end_date");
          final String user_id = map.get("userid");
          final int n = Integer.valueOf("n");
				  System.out.println(start_date + " " + end_date + " " + user_id);
          String response;

          String info = String.format("%s,%s\n", TEAM_ID, AWS_ACCOUNT_ID);
          StringBuilder sb = new StringBuilder();
          sb.append(info);
          //get tweets from this user
          Get g = new Get(Bytes.toBytes(user_id));
          try {
            final long start_time = System.currentTimeMillis();
            // System.out.println("mills taken before backend:" + (start_time - req_start));
            Result rr =table.get(g);

            String tweets = Bytes.toString(rr.getValue(Bytes.toBytes("a"),Bytes.toBytes("text")));
            String[] tweet_list = tweets.split("[####&&&&]");
            StringBuilder pos = new StringBuilder();
            ArrayList posList = new ArrayList();
            StringBuilder neg = new StringBuilder();
            ArrayList negList = new ArrayList();
            SimpleDateFormat f = new SimpleDateFormat("yyyy-mm-dd");
            long startdate = f.parse(start_date).getTime();
            long enddate = f.parse(end_date).getTime();
            for (int i=0;i<tweet_list.length;i++) {
                  String[] units = tweet_list[i].split("(@@@@****)");
                  String tweet_id = units[0];
                  long date = Float.valueOf(units[1]).longValue();
                  if (date < startdate || date > enddate) {
                      continue;
                  }
                  String text = units[2];
                  String score = units[3];
                  Tweet t = new Tweet(tweet_id, text, score, f.format(date));
                  if (Integer.valueOf(score) > 0) {
                      posList.add(t);
                  } else if (Integer.valueOf(score) < 0) {
                      negList.add(t);
                  }
            }

            Collections.sort(posList);
            Collections.sort(negList);
            if (posList.size() > 0) {
                pos.append("Positive Tweets\n");
            }
            if (negList.size() > 0) {
                neg.append("Negative Tweets\n");
            }
            for (int i=0;i<posList.size();i++) {
                Tweet tw = (Tweet) posList.get(i);
                pos.append(String.format("%s,%s,%s,%s\n",tw.
                date,tw.score,tw.id,tw.text));
            }
            for (int i=0;i<negList.size();i++) {
                Tweet tw = (Tweet) negList.get(i);
                neg.append(String.format("%s,%s,%s,%s\n",tw.
                date,tw.score,tw.id,tw.text));
            }

            if (posList.size() > 0 && negList.size() >0) {
              sb.append(pos.toString()).append("\n").append(neg.toString());
            } else {
              sb.append(pos.toString()).append(neg.toString());
            }

            final long end_time = System.currentTimeMillis();
            System.out.println("mills taken for backend:" + (end_time - start_time));

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
            // final long req_end = System.currentTimeMillis();
            // System.out.println("mills taken after backend:" + (req_end - end_time));
          } catch (IOException e) {
                e.printStackTrace();
          } catch (Exception e) {
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

  public static class Tweet implements Comparator {
     public String text;
     public String id;
     public String score;
     public String date;

     public Tweet(String id, String text, String score, String date) {
        this.id = id;
        this.text= text;
        this.score = score;
        this.date = date;
     }

     public int compare(Object obj1, Object obj2) {
          int p1 = Math.abs(Integer.valueOf(((Tweet) obj1).score));
          int p2 = Math.abs(Integer.valueOf(((Tweet) obj2).score));
          if (p1 > p2) {
               return 1;
           } else if (p1 < p2){
               return -1;
           } else {
               return 0;
           }
     }
  }
}
