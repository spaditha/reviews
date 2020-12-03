import com.google.gson.*;
import java.io.IOException;
import java.util.regex.*;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.*;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Counter;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import java.util.StringTokenizer; 
import java.util.ArrayList;
import java.util.Arrays; 
import java.util.Collections;
/**
 * * This Map-Reduce code will go through every Amazon review in rfox12:reviews It will then output
 * data on the top-level JSON keys
 */
public class my_amazon_mapreduce extends Configured implements Tool {
  // Just used for logging
  protected static final Logger LOG = LoggerFactory.getLogger(my_amazon_mapreduce.class);

  // This is the execution entry point for Java programs
  public static void main(String[] args) throws Exception {
    int res = ToolRunner.run(HBaseConfiguration.create(), new my_amazon_mapreduce(), args);
    System.exit(res);
  }

  public int run(String[] args) throws Exception {
    if (args.length != 1) {
      System.err.println("Need 1 argument (hdfs output path), got: " + args.length);
      return -1;
    }

    // Now we create and configure a map-reduce "job"
    Job job = Job.getInstance(getConf(), "my_amazon_mapreduce");
    job.setJarByClass(my_amazon_mapreduce.class);

    // By default we are going to can every row in the table
    Scan scan = new Scan();
    scan.setCaching(500); // 1 is the default in Scan, which will be bad for MapReduce jobs
    scan.setCacheBlocks(false); // don't set to true for MR jobs

    // This helper will configure how table data feeds into the "map" method
    TableMapReduceUtil.initTableMapperJob(
        "rfox12:reviews", // input HBase table name
        scan, // Scan instance to control CF and attribute selection
        MapReduceMapper.class, // Mapper class
        Text.class, // Mapper output key
        IntWritable.class, // Mapper output value
        job, // This job
        true // Add dependency jars (keep this to true)
        );

    // Specifies the reducer class to used to execute the "reduce" method after "map"
    job.setReducerClass(MapReduceReducer.class);

    // For file output (text -> number)
    FileOutputFormat.setOutputPath(
        job, new Path(args[0])); // The first argument must be an output path
    job.setOutputKeyClass(Text.class);
    job.setOutputValueClass(IntWritable.class);

    // What for the job to complete and exit with appropriate exit code
    return job.waitForCompletion(true) ? 0 : 1;
  }

  public static class MapReduceMapper extends TableMapper<Text, IntWritable> {
    private static final Logger LOG = LoggerFactory.getLogger(MapReduceMapper.class);

    // Here are some static (hard coded) variables
    private static final byte[] CF_NAME = Bytes.toBytes("cf"); // the "column family" name
    private static final byte[] QUALIFIER = Bytes.toBytes("review_data"); // the column name
    private static final IntWritable one =
        new IntWritable(1); // a representation of "1" which we use frequently

    private Counter rowsProcessed; // This will count number of rows processed
    private JsonParser parser; // This gson parser will help us parse JSON

    // This setup method is called once before the task is started
    @Override
    protected void setup(Context context) {
      parser = new JsonParser();
      rowsProcessed = context.getCounter("my_amazon_mapreduce", "Rows Processed");
    }

    // This "map" method is called with every row scanned.
    @Override
    public void map(ImmutableBytesWritable rowKey, Result value, Context context)
        throws InterruptedException, IOException {
      try {
        // Here we get the json data (stored as a string) from the appropriate column
        String jsonString = new String(value.getValue(CF_NAME, QUALIFIER));

        // Now we parse the string into a JsonElement so we can dig into it
        JsonElement jsonTree = parser.parse(jsonString);
        JsonObject jsonObject = jsonTree.getAsJsonObject();

        // My code example #####################################################################################
        String reviewrID = jsonObject.get("reviewerID").getAsString();
        double overallrating = jsonObject.get("overall").getAsDouble();
        boolean verified = jsonObject.get("verified").getAsBoolean();
        String review_text = jsonObject.get("reviewText").getAsString();
        StringTokenizer st = new StringTokenizer(review_text);
        
        
          /*context.write(new Text("Bucket Ranges:" + "\n" +
         "Bucket 1: [0,50]" + "\n" +
          "Bucket 2: (50,100]" + "\n" +
          "Bucket 3: (100,150]" + "\n" +
          "Bucket 4: (150,200]" + "\n" +
          "Bucket 5: (200,250]" + "\n" +
          "Bucket 6: (250,300]" + "\n" +
          "Bucket 7: (300,350]" + "\n" +
          "Bucket 8: (350,400]" + "\n" +
          "Bucket 9: (400,450]" + "\n" +
          "Bucket 10: (450,450>]" + "\n") , one);*/
        
        if((overallrating == 1.0  || overallrating == 2.0) && verified == true){
            
            if(st.countTokens() <= 50){context.write(new Text("Bucket: 1" + "\n" + "Verified Negative Reviews: true" + "\nIs Verified: " + verified + "\n" + "Counts: " + "\n"), one);}
              else if(st.countTokens() > 50 && st.countTokens() <= 100){context.write(new Text("Bucket: 2" + "\n" + "Is Negative Reviews: true" + "\nIs Verified: " + verified + "\n" + "Counts: " + "\n"), one);}
              else if(st.countTokens() > 100 && st.countTokens() <= 150){context.write(new Text("Bucket: 3" + "\n" + "Is Negative Reviews: true" + "\nIs Verified: " + verified + "\n" + "Counts: " + "\n"), one);}
              else if(st.countTokens() > 150 && st.countTokens() <= 200){context.write(new Text("Bucket: 4" + "\n" + "Is Negative Reviews: true" + "\nIs Verified: " + verified + "\n" + "Counts: " + "\n"), one);}
              else if(st.countTokens() > 200 && st.countTokens() <= 250){context.write(new Text("Bucket: 5" + "\n" + "Is Negative Reviews: true" +  "\nIs Verified: " + verified + "\n" + "Counts: " + "\n"), one);}
              else if(st.countTokens() > 250 && st.countTokens() <= 300){context.write(new Text("Bucket: 6" + "\n" + "Is Negative Reviews: true" + "\nIs Verified: " + verified + "\n" + "Counts: " + "\n"), one);}
              else if(st.countTokens() > 300 && st.countTokens() <= 350){context.write(new Text("Bucket: 7" + "\n" + "Is Negative Reviews: true" + "\nIs Verified: " + verified + "\n" + "Counts: " + "\n"), one);}
              else if(st.countTokens() > 350 && st.countTokens() <= 400){context.write(new Text("Bucket: 8" + "\n" + "Is Negative Reviews: true" + "\nIs Verified: " + verified + "\n" + "Counts: " + "\n"), one);}
              else if(st.countTokens() > 400 && st.countTokens() <= 450){context.write(new Text("Bucket: 9" + "\n" + "Is Negative Reviews: true" + "\nIs Verified: " + verified + "\n" + "Counts: " + "\n"), one);}
              else{context.write(new Text("Bucket: 10" + "\n" + "Is Negative Reviews: true" + "\nIs Verified: " + verified + "\n" + "Counts: " + "\n"), one);}
   
              
              
        } else if ((overallrating == 1.0  || overallrating == 2.0) && verified == false){
            
            if(st.countTokens() <= 50){ context.write(new Text("Bucket: 1" + "\n" + "Is Negative Reviews: true" + "\nIs Verified: " + verified + "\n" + "Counts: " + "\n"), one);}
              else if(st.countTokens() > 50 && st.countTokens() <= 100){context.write(new Text("Bucket: 2" + "\n" + "Is Negative Reviews: true" + "\nIs Verified: " + verified + "\n" + "Counts: " + "\n"), one);}
              else if(st.countTokens() > 100 && st.countTokens() <= 150){context.write(new Text("Bucket: 3" + "\n" + "Is Negative Reviews: true" + "\nIs Verified: " + verified + "\n" + "Counts: " + "\n"), one);}
              else if(st.countTokens() > 150 && st.countTokens() <= 200){context.write(new Text("Bucket: 4" + "\n" + "Is Negative Reviews: true" + "\nIs Verified: " + verified + "\n" + "Counts: " + "\n"), one);}
              else if(st.countTokens() > 200 && st.countTokens() <= 250){context.write(new Text("Bucket: 5" + "\n" + "Is Negative Reviews: true" + "\nIs Verified: " + verified + "\n" + "Counts: " + "\n"), one);}
              else if(st.countTokens() > 250 && st.countTokens() <= 300){context.write(new Text("Bucket: 6" + "\n" + "Is Negative Reviews: true" + "\nIs Verified: " + verified + "\n" + "Counts: " + "\n"), one);}
              else if(st.countTokens() > 300 && st.countTokens() <= 350){context.write(new Text("Bucket: 7" + "\n" + "Is Negative Reviews: true" + "\nIs Verified: " + verified + "\n" + "Counts: " + "\n"), one);}
              else if(st.countTokens() > 350 && st.countTokens() <= 400){context.write(new Text("Bucket: 8" + "\n" + "Is Negative Reviews: true" + "\nIs Verified: " + verified + "\n" + "Counts: " + "\n"), one);}
              else if(st.countTokens() > 400 && st.countTokens() <= 450){context.write(new Text("Bucket: 9" + "\n" + "Is Negative Reviews: true" + "\nIs Verified: " + verified + "\n" + "Counts: " + "\n"), one);}
              else{context.write(new Text("Bucket: 10" + "\n" + "Is Negative Reviews: true" + "\nIs Verified: " + verified + "\n" + "Counts: " + "\n"), one);}
           
           
            
        } else if ((overallrating == 4.0 || overallrating == 5.0) && verified == false){
            
            if(st.countTokens() <= 50){ context.write(new Text("Bucket: 1" + "\n" + "Is Positive Reviews: true" + "\nIs Verified: " + verified + "\n" + "Counts: " + "\n"), one);}
              else if(st.countTokens() > 50 && st.countTokens() <= 100){context.write(new Text("Bucket: 2" + "\n" + "Is Positive Reviews: true" + "\nIs Verified: " + verified + "\n" + "Counts: " + "\n"), one);}
              else if(st.countTokens() > 100 && st.countTokens() <= 150){context.write(new Text("Bucket: 3" + "\n" + "Is Positive Reviews: true" + "\nIs Verified: " + verified + "\n" + "Counts: " + "\n"), one);}
              else if(st.countTokens() > 150 && st.countTokens() <= 200){context.write(new Text("Bucket: 4" + "\n" + "Is Positive Reviews: true" + "\nIs Verified: " + verified + "\n" + "Counts: " + "\n"), one);}
              else if(st.countTokens() > 200 && st.countTokens() <= 250){context.write(new Text("Bucket: 5" + "\n" + "Is Positive Reviews: true" + "\nIs Verified: " + verified + "\n" + "Counts: " + "\n"), one);}
              else if(st.countTokens() > 250 && st.countTokens() <= 300){context.write(new Text("Bucket: 6" + "\n" + "Is Positive Reviews: true" + "\nIs Verified: " + verified + "\n" + "Counts: " + "\n"), one);}
              else if(st.countTokens() > 300 && st.countTokens() <= 350){context.write(new Text("Bucket: 7" + "\n" + "Is Positive Reviews: true" + "\nIs Verified: " + verified + "\n" + "Counts: " + "\n"), one);}
              else if(st.countTokens() > 350 && st.countTokens() <= 400){context.write(new Text("Bucket: 8" + "\n" + "Is Positive Reviews: true" + "\nIs Verified: " + verified + "\n" + "Counts: " + "\n"), one);}
              else if(st.countTokens() > 400 && st.countTokens() <= 450){context.write(new Text("Bucket: 9" + "\n" + "Is Positive Reviews: true" + "\nIs Verified: " + verified + "\n" + "Counts: " + "\n"), one);}
              else{context.write(new Text("Bucket: 10" + "\n" + "Is Positive Reviews: true" + "\nIs Verified: " + verified + "\n" + "Counts: " + "\n"), one);}
         
            
            
        }else if ((overallrating == 4.0 || overallrating == 5.0) && verified == true){
            
            if(st.countTokens() <= 50){ context.write(new Text("Bucket: 1" + "\n" + "Is Positive Reviews: true" + "\nIs Verified: " + verified + "\n" + "Counts: " + "\n"), one);}
              else if(st.countTokens() > 50 && st.countTokens() <= 100){context.write(new Text("Bucket: 2" + "\n" + "Is Positive Reviews: true" + "\nIs Verified: " + verified + "\n" + "Counts: " + "\n"), one);}
              else if(st.countTokens() > 100 && st.countTokens() <= 150){context.write(new Text("Bucket: 3" + "\n" + "Is Positive Reviews: true" + "\nIs Verified: " + verified + "\n" + "Counts: " + "\n"), one);}
              else if(st.countTokens() > 150 && st.countTokens() <= 200){context.write(new Text("Bucket: 4" + "\n" + "Is Positive Reviews: true" + "\nIs Verified: " + verified + "\n" + "Counts: " + "\n"), one);}
              else if(st.countTokens() > 200 && st.countTokens() <= 250){context.write(new Text("Bucket: 5" + "\n" + "Is Positive Reviews: true" + "\nIs Verified: " + verified + "\n" + "Counts: " + "\n"), one);}
              else if(st.countTokens() > 250 && st.countTokens() <= 300){context.write(new Text("Bucket: 6" + "\n" + "Is Positive Reviews: true" + "\nIs Verified: " + verified + "\n" + "Counts: " + "\n"), one);}
              else if(st.countTokens() > 300 && st.countTokens() <= 350){context.write(new Text("Bucket: 7" + "\n" + "Is Positive Reviews: true" + "\nIs Verified: " + verified + "\n" + "Counts: " + "\n"), one);}
              else if(st.countTokens() > 350 && st.countTokens() <= 400){context.write(new Text("Bucket: 8" + "\n" + "Is Positive Reviews: true" + "\nIs Verified: " + verified + "\n" + "Counts: " + "\n"), one);}
              else if(st.countTokens() > 400 && st.countTokens() <= 450){context.write(new Text("Bucket: 9" + "\n" + "Is Positive Reviews: true" + "\nIs Verified: " + verified + "\n" + "Counts: " + "\n"), one);}
              else{context.write(new Text("Bucket: 10" + "\n" + "Is Positive Reviews: true" + "\nIs Verified: " + verified + "\n" + "Counts: " + "\n"), one);}
         
      
           
            
        }
             
      
        
     
        
         

     
        
//           if(overallrating == 1.0 && verified == true){
   
//               context.write(new Text("Verified Negative Reviews: " + "\nRating: " + overallrating + "\nIs Verified: " + verified + "\n" + "Counts: " + "\n"), one);
              
//         } else if (overallrating == 1.0 && verified == false){
           
//             context.write(new Text("Non-Verified Negative Reviews: " + "\nRating: " + overallrating + "\nIs Verified: " + verified + "\n" + "Counts: " + "\n"), one);
            
//         } else if (overallrating == 2.0 && verified == true){
          
//             context.write(new Text("Verified Negative Reviews: " + "\nRating: " + overallrating + "\nIs Verified: " + verified + "\n" + "Counts: " + "\n"), one);
            
//         } else if (overallrating == 2.0 && verified == false){
        
//               context.write(new Text("Non-Verified Negative Reviews: " + "\nRating: " + overallrating + "\nIs Verified: " + verified + "\n" + "Counts: " + "\n"), one);
            
//         }else if (overallrating == 4.0 && verified == false){
         
//             context.write(new Text("Non-Verified Positive Reviews: " + "\nRating: " + overallrating + "\nIs Verified: " + verified + "\n" + "Counts: " + "\n"), one);
            
//         }else if (overallrating == 4.0 && verified == true){
      
//             context.write(new Text("Verified Positive Reviews: " + "\nRating: " + overallrating + "\nIs Verified: " + verified + "\n" + "Counts: " + "\n"), one);
            
//         }else if (overallrating == 5.0 && verified == false){
            
        
//             context.write(new Text("Non-Verified Positive Reviews: " + "\nRating: " + overallrating + "\nIs Verified: " + verified + "\n" + "Counts: " + "\n"), one);
            
//         }else if (overallrating == 5.0 && verified == true){
   
//              context.write(new Text("Verified Positive Reviews: " + "\nRating: " + overallrating + "\nIs Verified: " + verified + "\n" + "Counts: " + "\n"), one);
//         }
        
        
//         if (verified == true){
//           context.write(new Text("----IS Verified: " + verified + "\nCounts:"), one);
//         }else{
//           context.write(new Text("----IS Verified: " + verified  + "\nCounts:"), one);
//         }
        
        
      
         
      
        
        
      
// ####################################################################################################################################################
       

        

        // Here we increment a counter that we can read when the job is done
      rowsProcessed.increment(1);
      } catch (Exception e) {
        LOG.error("Error in MAP process: " + e.getMessage(), e);
      }
    }
  }

  // Reducer to simply sum up the values with the same key (text)
  // The reducer will run until all values that have the same key are combined
  public static class MapReduceReducer extends Reducer<Text, IntWritable, Text, IntWritable> {
    @Override
    public void reduce(Text key, Iterable<IntWritable> counts, Context context)
        throws IOException, InterruptedException {
      int sum = 0;
      for (IntWritable count : counts) {
        sum += count.get();
      }
      context.write(key, new IntWritable(sum));
    }
  }
}
