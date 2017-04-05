package HadoopFFT;

import java.io.IOException;

import org.apache.hadoop.conf.*;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

public class HadoopFFT {
	
	public static final int POINT = 64;
	
	enum Counter {
		LINESKIP, // error line
	}
	
	// map task
	// Text -> String; LongWritable -> long; IntWritable -> int; NullWritable -> null
    public static class FFTMaper extends Mapper<LongWritable, ShortArrayWritable, LongWritable, LongWritable> {
        public void map(LongWritable key, ShortArrayWritable value, Context context) throws IOException, InterruptedException {
        	//System.out.println(value.getShortNumber(0));
        	long temp = value.getShortNumber(0);
        	LongWritable b = new LongWritable();
        	b.set(temp);
        	context.write(key, b);
        }
    }

    public static class FFTReducer extends Reducer<LongWritable, LongWritable, LongWritable, LongWritable> {
    	// setNumReduceTasks() to set the number of reduce tasks
        public void reduce(LongWritable key, LongWritable value, Context context) throws IOException, InterruptedException {
        	// insert in here
        	LongWritable result = new LongWritable();
        	//System.out.println(value);
        	context.write(key, value);
        }
    }

    public static void main(String[] args) throws Exception {
    	Configuration conf = new Configuration();
    	//conf.set("mapred.job.tracker", "localhost:9001");

    	String[] newArgs = new String[] {"hdfs://localhost:9000/user/ubuntu/input", "hdfs://localhost:9000/user/ubuntu/output"};
    	String[] otherArgs = new GenericOptionsParser(conf, newArgs).getRemainingArgs();
    	if (otherArgs.length != 2) {
    		System.err.println("Usage: HadoopFFT <int> <out>");
    		System.exit(2);
    	}
    	
    	Job job = new Job(conf, "HadoopFFT");
    	//job.setJarByClass(HadoopFFT.class);
    	job.setMapperClass(FFTMaper.class);
    	//job.setCombinerClass(FFTReducer.class);
    	job.setReducerClass(FFTReducer.class);
    	
    	job.setInputFormatClass(ShortInputFormat.class);
    	job.setMapOutputKeyClass(LongWritable.class);
    	job.setMapOutputValueClass(LongWritable.class);
    	
    	job.setOutputKeyClass(LongWritable.class);
    	job.setOutputValueClass(LongWritable.class);
    	
    	FileInputFormat.addInputPath(job, new Path(otherArgs[0]));
    	FileOutputFormat.setOutputPath(job, new Path(otherArgs[1]));
    	
    	System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
    

}