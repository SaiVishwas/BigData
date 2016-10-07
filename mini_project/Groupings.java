import java.io.IOException;
import java.util.StringTokenizer;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class Groupings {

  public static class TokenizerMapper
       extends Mapper<Object, Text, Text, IntWritable>{

    private static IntWritable bat_score_assigned = new IntWritable(1);
    private Text word = new Text();

    public void map(Object key, Text value, Context context
                    ) throws IOException, InterruptedException {

    
    String avg = value.toString().trim().split(",")[7];
    String balls_faced = value.toString().trim().split(",")[8];
    String strike_rate = value.toString().trim().split(",")[9];
    String name = value.toString().trim().split(",")[0];
    
    Double bat_score = (Double.parseDouble(avg)*0.5 + Double.parseDouble(strike_rate)*0.3 + Double.parseDouble(balls_faced)*0.2) ;
    String batsman_score =  bat_score.intValue() + "" ;

/*
    Integer bat_score = (Integer.parseInt(avg) + Integer.parseInt(strike_rate) + Integer.parseInt(balls_faced)) ;
    String batsman_score =  bat_score + "" ;
*/        
    bat_score_assigned = new IntWritable(Integer.parseInt(batsman_score));
    
    word.set(name);
    context.write(word,bat_score_assigned);
      
    }
  }

  public static class IntSumReducer
       extends Reducer<Text,IntWritable,Text,IntWritable> {
    private IntWritable result = new IntWritable();
    
	int max = 0;	
    private Text max_key = new Text();
    
    public void reduce(Text key, Iterable<IntWritable> values,
                       Context context
                       ) throws IOException, InterruptedException {
    
    
        int sum = 0;
        

        for (IntWritable val : values) {
        sum += val.get();

	    }
	/*    if(sum > max )
	    {
	        max = sum;
                max_key = key ;   	
	    }	
	  */  
	    
	    result.set(sum);
	    context.write(key , result);
	
/*	max_key = key;
	max = values[0];
*/
	
      }
/*
	@Override
        protected void cleanup(Context context) throws IOException, InterruptedException {
	result.set(max);
        context.write(max_key, result);
     }
*/
  }
  
  public static void main(String[] args) throws Exception {
    Configuration conf = new Configuration();
    Job job = Job.getInstance(conf, "grouping players");
    job.setJarByClass(Groupings.class);
    job.setMapperClass(TokenizerMapper.class);
    job.setCombinerClass(IntSumReducer.class);
    job.setReducerClass(IntSumReducer.class);
    job.setOutputKeyClass(Text.class);
    job.setOutputValueClass(IntWritable.class);
    FileInputFormat.addInputPath(job, new Path(args[0]));
    FileOutputFormat.setOutputPath(job, new Path(args[1]));
    System.exit(job.waitForCompletion(true) ? 0 : 1);
  }
}
