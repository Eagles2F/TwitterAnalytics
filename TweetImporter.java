import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.io.FileInputStream;
import java.io.InputStreamReader;
import java.nio.charset.Charset;
import java.io.PrintStream;

public class TweetImporter {

  public static void importTweets(String file) {
    try (BufferedReader br = new BufferedReader(new InputStreamReader(new FileInputStream(file), "UTF8"))) {
      String line;
      while ((line = br.readLine()) != null) {
        Utf8Stream.println(line);
        String[] fields = line.split("\\t");
        // tweetId, userId, time, text, score
        TweetDatastore.insertTweet(fields[0], fields[1], 
            fields[2], 
            Integer.valueOf(fields[3]));
      }
    } catch (IOException e) {
      e.printStackTrace();
    }
  }

  public static void main(String[] args) {
    TweetImporter.importTweets("tweets");
  }
}
