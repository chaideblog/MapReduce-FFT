package HadoopFFT;

import java.io.IOException;

import org.apache.hadoop.conf.*;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

public class HadoopFFT {
	
	public static final int POINT = 64;
	
	enum Counter {
		LINESKIP, // error line
	}
	
	// map task
    public static class FFTMaper extends Mapper<LongWritable, ShortArrayWritable, LongWritable, ShortArrayWritable> {
        public void map(LongWritable key, ShortArrayWritable value, Context context) throws IOException, InterruptedException {
        	
        	//LongWritable result = new LongWritable();
        	//System.out.println(value.getShortNumber(0));
        	//result.set(value.getShortNumber(0));
        	
        	//Text text = new Text("hadoop");
        	context.write(key, value);
        }
    }
/*
    public static class FFTReducer extends Reducer<LongWritable, ShortArrayWritable, LongWritable, LongWritable> {
    	// setNumReduceTasks() to set the number of reduce tasks
        public void reduce(LongWritable key, ShortArrayWritable value, Context context) throws IOException, InterruptedException {
        	// insert in here
        	LongWritable result = new LongWritable();
        	short temp = value.getShortNumber(0);
        	result.set(temp);
        	//System.out.println(value);
        	context.write(key, result);
        }
    }
*/
    public static void main(String[] args) throws Exception {
    	Configuration conf = new Configuration();
    	Job job = new Job(conf, "HadoopFFT");
    	job.setJarByClass(HadoopFFT.class);
    	
    	String[] newArgs = new String[] {"hdfs://localhost:9000/input", "hdfs://localhost:9000/output"};
    	String[] otherArgs = new GenericOptionsParser(conf, newArgs).getRemainingArgs();
    	if (otherArgs.length != 2) {
    		System.err.println("Usage: HadoopFFT <int> <out>");
    		System.exit(2);
    	}
    	
    	FileInputFormat.addInputPath(job, new Path(otherArgs[0]));
    	FileOutputFormat.setOutputPath(job, new Path(otherArgs[1]));
    	
    	job.setMapperClass(FFTMaper.class);
    	//job.setCombinerClass(FFTReducer.class);
    	//job.setReducerClass(FFTReducer.class);
    	
    	
    	job.setInputFormatClass(ShortInputFormat.class);
    	job.setOutputFormatClass(ShortOutputFormat.class);
    	//job.setMapOutputKeyClass(LongWritable.class);
    	//job.setMapOutputValueClass(ShortArrayWritable.class);
    	
    	job.setOutputKeyClass(LongWritable.class);
    	job.setOutputValueClass(ShortArrayWritable.class);
    	
    	System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
    

}