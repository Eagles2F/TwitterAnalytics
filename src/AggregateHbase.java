import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.PriorityQueue;

/**
 * Created by jindaxian on 11/9/15.
 */
public class AggregateHbase {
    final static String SM = "lmsh1ge6opytrg12dserfgv";
    final static String DM = "asgdhjbf673bvsalfjoq3ng";

    public static class HbaseMapper
            extends Mapper<Object, Text, Text, Text>{

        //private final static IntWritable one = new IntWritable(1);
        private Text word = new Text();
        private Text w_value= new Text();

        public void map(Object key, Text value, Mapper.Context context
             ) throws IOException, InterruptedException {
            String content=value.toString();

            String[] values = value.toString().split("\t");
            int count = Integer.parseInt(values[2]);
            String text = values[4];
            String tag = values[5];
            String date = values[1];
            String userIDList = values[3];

            String output = date + SM + count + SM + userIDList + SM + text;
            word.set(tag);
            w_value.set(output);
            context.write(word, w_value);
        }
    }
public static class HbaseComniner
        extends Reducer<Text,Text,Text,Text>{
    private Text result = new Text();

    public void reduce(Text key, Iterable<Text> values,
        Context context
            ) throws IOException, InterruptedException {
        String output = "";

        for(Text text : values){
            String part = text.toString();
            output += (part + DM);
        }
        output = output.substring(0,output.length() - DM.length());

        result.set(output);
        context.write(key, result);
    }
}

public static class HaseReducVer
        extends Reducer<Text,Text,Text,Text> {
    private Text result = new Text();
    private Text keyOut = new Text();

    static class OneItem implements Comparable<OneItem> {
        public int count;
        public String date;
        String others;
        String userlist;
        String text;
        public OneItem(int count, String date, String userlist, String text){
           this.count = count;
            this.date = date;
            this.text = text;
            this.userlist = userlist;
        }
        public int compareTo(OneItem another){
            if(count != another.count){
                return count - another.count;
            }else{
                return date.compareTo(another.date);
            }
        }
        public String toString(){
            return String.format("%s:%s:%s:%s", date, count, userlist, text);
        }
    }
    public void reduce(Text key, Iterable<Text> values,
                       Context context
    ) throws IOException, InterruptedException {
        PriorityQueue<OneItem> q = new PriorityQueue<OneItem>();
        for (Text value : values){
            String[] eachVal = value.toString().split(DM);
            for(String each : eachVal) {
                String[] secs = each.split(SM);
                String date = secs[0];
                int count = Integer.parseInt(secs[1]);
                String userlist = secs[2];
                String text = secs[3];
                q.offer(new OneItem(count, date, userlist, text));
            }
        }
        String output = "";
        while(!q.isEmpty()){
            output += (q.poll().toString() + DM );
        }
        result.set(output.substring(0, output.length()-DM.length()));
        context.write(key, result);
    }
}
}
