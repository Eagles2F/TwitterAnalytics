import com.google.gson.Gson;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.*;

public class Aggregate {
	
	public static String escape(String txt){
	    
	  	txt = txt.replace("\\", "\\\\");
	    txt = txt.replace("\n", "\\n");
	    txt = txt.replace("\t", "\\t");
	    txt = txt.replace("\r", "\\r");
	    txt = txt.replace("\f", "\\f");
	    txt = txt.replace("\b", "\\b");
	    txt = txt.replace("\'", "\\\'");
	    txt = txt.replace("\"", "\\\"");
	    return txt;
	}
	public static String unescape(String txt){
	   	txt = txt.replace("\\\\", "\\");
	    txt = txt.replace("\\n", "\n");
	    txt = txt.replace("\\t", "\t");
	    txt = txt.replace("\\r", "\r");
	    txt = txt.replace("\\f", "\f");
	    txt = txt.replace("\\b", "\b");
	    txt = txt.replace("\\\'", "\'");
	    txt = txt.replace("\\\"", "\"");
	    return txt;
	}

	 public static class TweetMapper
     extends Mapper<Object, Text, Text, Text>{

  //private final static IntWritable one = new IntWritable(1);
  private Text word = new Text();
  private Text w_value= new Text();

  

  public void map(Object key, Text value, Context context
                  ) throws IOException, InterruptedException {
    String content=value.toString();
    
    HashMap<String, Object> map = new Gson().fromJson(content, HashMap.class);
    
    Map<String, Object> entities = (Map<String, Object>)map.get("entities");
    String txt = (String)map.get("text");
    String createdAt = (String)map.get("created_at");
    
    /*
     * Extract tweet ID
     */
    String tweetID = (String)map.get("id_str");
    if(tweetID == null){
    	Double did = (Double)map.get("id");
    	if(did == null) return;
    	tweetID = Double.toString(did);
    }
    
    Map<String, Object> user = (Map<String, Object>)map.get("user");
    
    if(entities == null || txt == null || createdAt == null || user == null || tweetID == null){
    	return;
    }
    /*
     * Extract and escape text here
     */
    txt = Aggregate.escape(txt);
    
    /*
     * extract user id here
     */
    String userID = null;
    String idStr = (String)user.get("id_str");
    if(idStr != null){
    	userID = idStr;
    }else{
    	Double idTmp = (Double)user.get("id");
    	if(idTmp == null) return;
    	
    	userID = Long.toString(idTmp.longValue());
    }
    
    if(userID == null) return;
    
    /*
     * Extract tags here
     */
    List<Map<String,Object>> hashtags = (ArrayList<Map<String, Object>>)entities.get("hashtags");
    if(hashtags == null || hashtags.size()==0) return;
    
    List<String> tagsTxt = new ArrayList<String>(); 
    for(Map<String, Object> tag : hashtags){
    	String tagTxt = (String)tag.get("text");
		tagTxt = escape(tagTxt);
    	if(tagTxt == null) continue;
    	tagsTxt.add(tagTxt);
    	//mapKeys.add(tagTxt + "," + date);
    }
    if(tagsTxt.size() == 0) return;
    
    /*
     * Parse createAt to date format
     */
    SimpleDateFormat inputFormat = new SimpleDateFormat("EEE MMM dd HH:mm:ss ZZZZZ yyyy", Locale.ENGLISH);
    inputFormat.setLenient(true);
    
    SimpleDateFormat outputFormat = new SimpleDateFormat("yyyy-MM-dd", Locale.ENGLISH);
    outputFormat.setLenient(true);
    outputFormat.setTimeZone(new SimpleTimeZone(0,"timezone"));
    
    String date = ""; 
    Date time = null;
    try {
		time = inputFormat.parse(createdAt);
		date = outputFormat.format(time);
	} catch (ParseException e) {
		// TODO Auto-generated catch block
		
	}
    
    /*
     * Get unix time
     */
    long unixTime = time.getTime();
    
     
    /*
     * Generate output 
     */
    String mapVal = "1" + "\t" + userID + "\t" + txt + "\t" + unixTime;
    //w_value.set(mapVal);
    w_value.set(mapVal);
    for(String tag : tagsTxt){
    	String outKey = tag + "," + date;  	
    	word.set(outKey);
    	
    	context.write(word,w_value);
    }
  }
  }


  public static class TweetCombiner
       extends Reducer<Text,Text,Text,Text> {
    private Text result = new Text();

    public void reduce(Text key, Iterable<Text> values,
                       Context context
                       ) throws IOException, InterruptedException {
		String keyString = key.toString();
		String tag = keyString.split(",")[0];
		String date = keyString.split(",")[1];

    	PriorityQueue<Long> useridList = new PriorityQueue<Long>();

    	String userIDStringList = "";
    	int count=0;
    	String finalText = null;
    	String text = "\0";
    	long earlyUnixTime = -1;
    	
    	for(Text value : values){
    		String content = value.toString();
    		
    		String[] parts = content.split("\t");
    		
    		/*
    		 * counting
    		 */

    		String num = parts[0];
    		count += Integer.parseInt(num);
    		
    		/*
    		 * Generate user id list 
    		 */
    		
    		String id = parts[1];
    		long lid = Long.parseLong(id);
    		useridList.offer(lid);
    		
    		
    		/*
    		 * Choose the text
    		 */
    		
    		String txt = unescape(parts[2]);
    		long curUnixTime = Long.parseLong(parts[3]);
    		
    		if(earlyUnixTime == -1){
    			earlyUnixTime = curUnixTime;
    			text = txt;
    			finalText = parts[2];
    		}else{
    			if(curUnixTime < earlyUnixTime){
    				earlyUnixTime = curUnixTime;
        			text = txt;
        			finalText = parts[2];
    			}else if(curUnixTime > earlyUnixTime){
    				continue;
    			}else{
    				if(text.compareTo(txt) < 0){
    					text = txt;
    					finalText = parts[2];
    				}
    			}
    		}
    		
    	}
    	
//    	for(String tid : tweetIDSet){
//    		tweetIDStringList += (tid+",");
//    	}
//    	tweetIDStringList = tweetIDStringList.substring(0, tweetIDStringList.length()-1);
    	
    	while(!useridList.isEmpty()){
    		userIDStringList += (useridList.poll()+",");
    	}
    	userIDStringList = userIDStringList.substring(0, userIDStringList.length()-1);
    	
    	String combineOutVal = count + "\t" + userIDStringList + "\t" + finalText + "\t" + earlyUnixTime ;
    	result.set(combineOutVal);
    	
    	context.write(key, result);
    }
  }
    
    public static class TweetReducer
    extends Reducer<Text,Text,Text,Text> {
    	private Text result = new Text();

    	public void reduce(Text key, Iterable<Text> values,
                    	Context context
                    	) throws IOException, InterruptedException {

    		PriorityQueue<Long> useridList = new PriorityQueue<Long>();
    		int count = 0;
    		String userIDStringList = "";

    		String finalText = null;
    		String text = "\0";
    		long earlyUnixTime = -1;

    		 String keyString = key.toString();
    		 String tag = keyString.split(",")[0];
    		 String date = keyString.split(",")[1];

    		for(Text value : values){
    			String content = value.toString();

    			String[] parts = content.split("\t");

    			/*
    			 * combine all tweet id
    			 */
				count += Integer.parseInt(parts[0]);
    			/*
    			 * Generate user id list
    			 */

    			String[] users = parts[1].split(",");

    			for(String id : users){
    				long lid = Long.parseLong(id);
    				useridList.offer(lid);
    			}

    			/*
    			 * Choose the text
    			 */

    			String txt = unescape(parts[2]);
    			long curUnixTime = Long.parseLong(parts[3]);

    			if(earlyUnixTime == -1){
    				earlyUnixTime = curUnixTime;
    				text = txt;
    				finalText = parts[2];
    			}else{
    				if(curUnixTime < earlyUnixTime){
    					earlyUnixTime = curUnixTime;
    					text = txt;
    					finalText = parts[2];
    				}else if(curUnixTime > earlyUnixTime){
    					continue;
    				}else{
    					if(text.compareTo(txt) < 0){
    						text = txt;
    						finalText = parts[2];
    					}
    				}
    			}

    		}

    		while(!useridList.isEmpty()){
    			userIDStringList += (useridList.poll()+",");
    		}
    		userIDStringList = userIDStringList.substring(0, userIDStringList.length()-1);

    		String reduceOutVal = date + "\t" + count + "\t" + userIDStringList + "\t" + finalText + "\t" + tag ;
    		result.set(reduceOutVal);

    		context.write(key, result);
    	}
    }
    public static void main(String args[]) throws Exception{
		 Configuration conf = new Configuration();
		 Job  job1 = new Job(conf, "TweetQ4");
		 job1.setJarByClass(Aggregate.class);
		 job1.setMapperClass(Aggregate.TweetMapper.class);
		 job1.setCombinerClass(Aggregate.TweetCombiner.class);
		 job1.setReducerClass(Aggregate.TweetReducer.class);

		 job1.setOutputKeyClass(Text.class);
		 job1.setOutputValueClass(Text.class);
		 //FileInputFormat.addInputPath(job, new Path("s3n://cmucc-datasets/twitter/f15/"));
		 //FileOutputFormat.setOutputPath(job, new Path("s3n://15619etlpurrito/outputq4/"));
		 FileInputFormat.addInputPath(job1,new Path(args[0]));
		 FileOutputFormat.setOutputPath(job1, new Path(args[1]));
		 job1.waitForCompletion(true);

		Job  job2 = new Job(conf, "TweetQ4Hbase");
		job2.setJarByClass(Aggregate.class);
		job2.setMapperClass(AggregateHbase.HbaseMapper.class);
		job2.setCombinerClass(AggregateHbase.HbaseComniner.class);
		job2.setReducerClass(AggregateHbase.HaseReducVer.class);
		job2.setOutputKeyClass(Text.class);
		job2.setOutputValueClass(Text.class);
		FileInputFormat.addInputPath(job2,new Path(args[1]));
		FileOutputFormat.setOutputPath(job2, new Path(args[2]));
		job2.waitForCompletion(true);
	}
  }

//  public static void  aggregate(String[] args) throws Exception {
//    Configuration conf = new Configuration();
//    Job job = new Job(conf, "Aggregates count");
//    job.setJarByClass(Aggregates.class);
//    job.setMapperClass(TokenizerMapper.class);
//    job.setCombinerClass(IntSumReducer.class);
//    job.setReducerClass(IntSumReducer.class);
//    job.setOutputKeyClass(Text.class);
//    job.setOutputValueClass(Text.class);
//    FileInputFormat.addInputPath(job, new Path(args[0]));
//    FileInputFormat.addInputPath(job, new Path(args[1]));
//    FileOutputFormat.setOutputPath(job, new Path(args[2]));
//    //System.exit(job.waitForCompletion(true) ? 0 : 1);
//  } 
