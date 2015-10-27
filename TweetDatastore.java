import java.sql.Connection;
import java.sql.Statement;
import java.sql.PreparedStatement;
import com.mchange.v2.c3p0.ComboPooledDataSource;
import java.sql.ResultSet;

import java.util.ArrayList;

public class TweetDatastore {

  private static final String MYSQL_DB_URL = "jdbc:mysql://127.0.0.1:3306";
  private static final String DATABASE_NAME = "purrito";
  private static final String TABLE_NAME = "tweets";
  private static final int MAX_POOL_SIZE = 30;
  private static final int MIN_POOL_SIZE = 3;
  private static final ComboPooledDataSource cpds = new ComboPooledDataSource();

  static {
    try {
      cpds.setDriverClass("com.mysql.jdbc.Driver"); // loads the jdbc driver
      cpds.setJdbcUrl(MYSQL_DB_URL+"/"+DATABASE_NAME);
      cpds.setUser("root");
      cpds.setPassword("");
      cpds.setMaxPoolSize(MAX_POOL_SIZE);
      cpds.setMinPoolSize(MIN_POOL_SIZE);
    } catch (Exception e) {
      e.printStackTrace();
    }
  }

  public static Connection getConnection() {
    Connection conn = null;
    try {
      conn = cpds.getConnection();
    } catch (Exception e) {
      e.printStackTrace();
    }
    return conn;
  }

  public static ArrayList<String> selectTweets(String userId, String timestamp) {
    ArrayList<String> tweets = new ArrayList<String>();
    String selectSql = "select tweetId, score, text from " + TABLE_NAME + " where userId=? and time=?;";
    try {
      Connection conn = getConnection();
      PreparedStatement ps = conn.prepareStatement(selectSql);
      ps.setString(1, userId);
      ps.setString(2, timestamp);
      ResultSet rs = ps.executeQuery();
      while (rs.next()) {
        String record = String.format("%s:%d:%s", rs.getString("tweetId"), 
            rs.getInt("score"), 
            rs.getString("text"));
        tweets.add(record);
      }
    } catch (Exception e) {
      e.printStackTrace();
    }
    return tweets;
  }

  public static void insertTweet(String tweetId, String userId, String timestamp, 
      String text, int score) {
    String insertSql = "insert into " + TABLE_NAME + " (tweetId, userId, time, text, score) VALUES " + 
      " (?, ?, ?, ?, ?);";
    try {
      Connection conn = getConnection();
      PreparedStatement ps = conn.prepareStatement(insertSql);
      ps.setString(1, tweetId);
      ps.setString(2, userId);
      ps.setString(3, timestamp);
      ps.setString(4, text);
      ps.setInt(5, score);
      ps.executeUpdate();
    } catch (Exception e) {
      e.printStackTrace();
    }
  }

  public static void selectAll() {
    try {
      Connection conn = getConnection();
      Statement statement = conn.createStatement();
      ResultSet rs = statement.executeQuery("SELECT * FROM " + TABLE_NAME);
      while (rs.next()) {
        String record = String.format("%s, %s, %s, %s, %d", rs.getString("tweetId"), 
            rs.getString("userId"), 
            rs.getString("time"), 
            rs.getString("text"), 
            rs.getInt("score"));
        Utf8Stream.println(record);
      }
    } catch (Exception e) {
      e.printStackTrace();
    }
  }

}
