import java.io.*;
import java.io.IOException;
import java.util.StringTokenizer;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.fs.FileSystem;

public class Matrix {
	//MapOne takes care of Amatrix
	public static class MapOne extends Mapper<Object, Text, Text, Text> {
    	public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
		    String key_val;
		    String[] values = value.toString().split(",");
		    for(int i = 0; i < values.length; ++i) 
		    	values[i] = values[i].trim();
		    
            for(int i = 0; i < 1; ++i) {
            	key_val = values[1] + "," + Integer.toString(i);
            	context.write(new Text(key_val), value); 
            }
    	}
  	}
	//MapOne takes care of Vmatrix
	public static class MapTwo extends Mapper<Object, Text, Text, Text>{
	    public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
		    String key_val;
		    String[] values = value.toString().split(",");
		    for(int i = 0; i < values.length; ++i) 
		    	values[i] = values[i].trim();
		    
			for(int i = 0; i < 4; ++i) {
		        key_val = Integer.toString(i) + "," + values[2];
		        context.write(new Text(key_val), value); 
		    }	    
    	}
  	}
	
	public static class MatrixSumReducer extends Reducer<Text,Text,Text,DoubleWritable> {
		public void reduce(Text key, Iterable<Text> values,Context context) throws IOException, InterruptedException {
			double sum = 0.0;
			double[] A = new double[4];
			double[] v = new double[4];
			
			for (Text value : values) {
				String[] data = value.toString().split(",");
				if(data[0].equals("a")) {
					A[Integer.parseInt(data[2])] = Double.parseDouble(data[3]);
				}
				else if(data[0].equals("v")) {
					v[Integer.parseInt(data[1])] = Double.parseDouble(data[3]);
				}
			}
			
			for(int i = 0; i < 4; ++i)
				sum += A[i] * v[i];
			
			String[] data = key.toString().split(",");
			String new_key = "v," + data[0] + "," + data[1] + ",";
			sum = (Math.round(sum*100))/100.0;
			context.write(new Text(new_key), new DoubleWritable(sum));
		}
	}
  
	public static boolean is_stable(Configuration conf, int index)throws IOException {
  
		double[] x = new double[4];
  		double[] x_new = new double[4];
  
  		FileSystem fs = FileSystem.get(conf);
		Path input_path = new Path("hdfs://localhost:9000/usr/matrix/file"+index+"/part-r-00000");
		Path output_path = new Path("hdfs://localhost:9000/usr/matrix/file"+(index + 1)+"/part-r-00000");
		BufferedReader br_input = new BufferedReader(new InputStreamReader(fs.open(input_path)));
        BufferedReader br_output = new BufferedReader(new InputStreamReader(fs.open(output_path)));
        String line;
        
		// Create Vector X
		while ((line = br_input.readLine()) != null) {
            String[] data = line.toString().split(",");
            if(data[0].trim().equals("v")) 
            	x[Integer.parseInt(data[1].trim())] = Double.parseDouble(data[3].trim());
        }
        
		// Create Vector X_New
        while ((line = br_output.readLine()) != null) {
            String[] data = line.toString().split(",");
            if(data[0].trim().equals("v")) 
            	x_new[Integer.parseInt(data[1].trim())] = Double.parseDouble(data[3].trim());
        }
	  	//compare
		for(int i = 0; i < 4; ++i) 
	  	 	if(x[i] != x_new[i]) 
	  	 		return false;

		return true;
	}

  	public static void main(String[] args) throws Exception {	  	
	  	Configuration conf = new Configuration();
	  	int i = 0;
	  	boolean complete = false;
	  	boolean stable = false;
	  	
		do {
			Job job = Job.getInstance(conf, "matrix multiplication");
			job.setJarByClass(Matrix.class);
			job.setReducerClass(MatrixSumReducer.class);
			job.setMapOutputKeyClass(Text.class);
			job.setMapOutputValueClass(Text.class);
			job.setOutputKeyClass(Text.class);
			job.setOutputValueClass(DoubleWritable.class);
			
			MultipleInputs.addInputPath(job, new Path("hdfs://localhost:9000/usr/trial/a_matrix"),TextInputFormat.class, MapOne.class);
			MultipleInputs.addInputPath(job, new Path("hdfs://localhost:9000/usr/trial/file"+i+"/part-r-00000"),TextInputFormat.class, MapTwo.class);
			FileOutputFormat.setOutputPath(job, new Path("hdfs://localhost:9000/usr/trial/file"+(i + 1)));
			complete = job.waitForCompletion(true);
			
			if(complete)
				stable = is_stable(conf, i++);

		}while(!stable);
	}
}
