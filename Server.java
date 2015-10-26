import org.vertx.java.core.Handler;
import org.vertx.java.core.http.HttpServerRequest;
import org.vertx.java.platform.Verticle;
import org.vertx.java.core.http.RouteMatcher;
import org.vertx.java.core.http.HttpServer;
import org.vertx.java.core.MultiMap;
import java.math.*;

import java.text.SimpleDateFormat;

public class Server extends Verticle {
  final static String teamId = "purrito";
  final static String accountId = "339035512528";
  final static String secretKey = "827199720896087247873518181557816672351992917789655884592225059551192139504912";


  public String decipher(String message, String key) {
    BigInteger big1 = new BigInteger(key);
    BigInteger big2 = new BigInteger(secretKey);
    BigInteger big3 = big1.divide(big2);

    int y = big3.intValue();

    int n = (int) Math.sqrt((double) message.size());

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
    for (int i = 0; i < intermediate.size(); i ++) {
        int order = (int) (intermediate.charAt(i) - 'A');
        if (order < zz) {
            sb2.append((String) ('Z' - (zz - order - 1)));
        } else {
            sb2.append((String) (intermediate.charAt(i) - zz));
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
        String response = String.format("%s,%s\n%s\n%s\n", teamId,
          accountId, timestamp, decipher(message, key));

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

        String response = String.format("%s,%s\n", teamId, accountId);

        req.response().putHeader("Content-Type", "text/plain");
        req.response().putHeader("Content-Length",
          String.valueOf(response.length()));
        req.response().end(response);
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
    server.listen(8080);
  }
}
