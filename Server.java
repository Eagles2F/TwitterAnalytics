import org.vertx.java.core.Handler;
import org.vertx.java.core.http.HttpServerRequest;
import org.vertx.java.platform.Verticle;
import org.vertx.java.core.http.RouteMatcher;
import org.vertx.java.core.http.HttpServer;
import org.vertx.java.core.MultiMap;
import java.math.*;
import java.util.ArrayList;

import java.text.SimpleDateFormat;


public class Server extends Verticle {
  private final static String TEAM_ID = "purrito";
  private final static String AWS_ACCOUNT_ID = "339035512528";
  private final static String SECRET_KEY = "8271997208960872478735181815578166723519929177896558845922250595511921395049126920528021164569045773";

  public String decipher(String message, String key) {
    BigInteger big1 = new BigInteger(key);
    BigInteger big2 = new BigInteger(SECRET_KEY);
    BigInteger big3 = big1.divide(big2);

    int y = big3.intValue();
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
    int zz = y % 25 + 1;
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

  public String unescape(String text) {
    if (text == null) {
      return null;
    }
    text = text.replace("\\n", "\n");
    text = text.replace("\\r", "\r");
    text = text.replace("\\t", "\t");
    text = text.replace("\\f", "\f");
    text = text.replace("\\b", "\b");
    text = text.replace("\\\'", "\'");
    text = text.replace("\\\"", "\"");
    text = text.replace("\\\\", "\\");
    return text;
  }

  public void start() {
    final RouteMatcher router = new RouteMatcher();
    final HttpServer server = vertx.createHttpServer();
		server.setAcceptBacklog(32767);
		server.setUsePooledBuffers(true);
		server.setReceiveBufferSize(4 * 1024);

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

    router.get("/q2", new Handler<HttpServerRequest>() {
			@Override
			public void handle(final HttpServerRequest req) {
				MultiMap map = req.params();
				final String userId = map.get("userid");
				final String tweetTime = map.get("tweet_time");
        ArrayList<String> tweets = TweetDatastore.selectTweets(userId, tweetTime);
        StringBuilder response = new StringBuilder(String.format("%s,%s\n", TEAM_ID, AWS_ACCOUNT_ID));
        for (String tweet : tweets) {
          tweet = unescape(tweet);
          response.append(tweet+"\n");
        }
        int length = 0;
        try {
          // string.length() return the numberofcharacters instead byte length
          length = response.toString().getBytes("utf-8").length;
        } catch (Exception e) {
          e.printStackTrace();
        }
        // specify charset in response
        req.response().putHeader("Connection", "close");
        req.response().putHeader("Content-Type", "text/plain;charset=utf-8");
        req.response().putHeader("Content-Length",
          String.valueOf(length));
        req.response().end(response.toString(), "utf-8");
			}
    });

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
