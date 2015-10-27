import java.io.PrintStream;
import java.io.IOException;

public class Utf8Stream {

  public static void println(String str) {
    PrintStream out = null;
    try {
      out = new PrintStream(System.out, true, "UTF-8");
      out.println(str);
    } catch (IOException e) {
      e.printStackTrace();
    }
  }
}
