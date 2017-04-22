package HadoopFFT;

import java.io.IOException;
import java.util.Iterator;
import org.apache.commons.math3.transform.DftNormalization;
import org.apache.commons.math3.transform.TransformType;
import org.apache.hadoop.conf.*;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;
import FFT.*;
import Input.*;
import Output.*;

public class HadoopFFT {
	public static final int POINT = 8 * 1024 * 1024; // 设置一次读入的点数
	
	enum Counter {
		LINESKIP, // 记录错误行数
	}
	
    public static class FFTMapper extends Mapper<LongWritable, FFTWritable, NullWritable, FFTWritable> {
    	/* Map程序
    	 * 输入：key: LongWritable, value: FFTWritable
    	 * 输出：key: NullWritable, value: FFTWritable
    	 * 功能：对读入的时域采样信号进行快速傅利叶变换。
    	 */
        public void map(LongWritable key, FFTWritable value, Context context) throws IOException, InterruptedException {
        	FFTWritable result = new FFTWritable();
        	fast_fourier_transformer fft = new fast_fourier_transformer(DftNormalization.STANDARD); // 构造快速傅利叶变换类
        	result = fft.transform(value, TransformType.FORWARD); // 前向快速傅利叶变换
        	context.write(NullWritable.get(), result); // 输出变换后的频域数据
        }
    }

    public static class FFTReducer extends Reducer<NullWritable, FFTWritable, NullWritable, Text> {
    	/* Reduce程序
    	 * 输入：key: NullWritable, value: FFTWritable
    	 * 输出：key: NullWritable, value: Text
    	 * 功能：对读入的信号的频域数据进行特征提取。
    	 */
        public void reduce(NullWritable key, Iterable<FFTWritable> values, Context context) throws IOException, InterruptedException {
        	Iterator<FFTWritable> iterator = values.iterator(); // 构造频域数据迭代器
        	FFTWritable value = new FFTWritable();
        	while (iterator.hasNext()) {
        		value = iterator.next();
        		
        		double sum = 0;
            	double average = 0;
            	int maxValue = 0xffffffff;
            	int maxLocation = -1;
            	int minValue = 0x7fffffff;
            	int minLocation = -1;
            	int temp = 0;
            	
	        	for (int count = 0; count < value.size(); count++) {
	        		temp = value.get(count).get();
	        		sum += temp;
	        		if (maxValue < temp) {
	        			maxValue = temp;
	        			maxLocation = count;
	        		}
	        		if (minValue > temp) {
	        			minValue = temp;
	        			minLocation = count;
	        		}
	        	}
	        	average = sum / value.size();
	        	String result = "Max = " + maxValue + " Max Location = " + maxLocation + 
	        			" Min = " + minValue+ " Min Location = " + minLocation + " Average = " + average + "\n";
	        	context.write(NullWritable.get(), new Text(result)); // 输出特征提取的结果
        	}
        }
    }

    public static void main(String[] args) throws Exception {
    	Configuration conf = new Configuration();
    	Job job = new Job(conf, "HadoopFFT"); // 设置任务名称
    	job.setJarByClass(HadoopFFT.class); // 设置jar包
    	
    	/********设置输入输出目录********/
    	//String[] newArgs = new String[] {"hdfs://xidian:9000/user/hadoop/input", "hdfs://xidian:9000/user/hadoop/output"};
    	String[] newArgs = new String[] {"hdfs://localhost:9000/input", "hdfs://localhost:9000/output"};
    	String[] otherArgs = new GenericOptionsParser(conf, newArgs).getRemainingArgs();
    	if (otherArgs.length != 2) {
    		System.err.println("Usage: HadoopFFT <int> <out>");
    		System.exit(2);
    	}
    	FileInputFormat.addInputPath(job, new Path(otherArgs[0]));
    	FileOutputFormat.setOutputPath(job, new Path(otherArgs[1]));
    	
    	/********设置Map、Reduce类********/
    	job.setMapperClass(FFTMapper.class);
    	//job.setCombinerClass(FFTReducer.class);
    	job.setReducerClass(FFTReducer.class);

    	/********设置Map输出格式********/
    	job.setMapOutputKeyClass(NullWritable.class);
    	job.setMapOutputValueClass(FFTWritable.class);
    	
    	/********设置任务输出格式********/
    	job.setOutputKeyClass(NullWritable.class);
    	job.setOutputValueClass(Text.class);
    	
    	/********设置任务输入、输出格式********/
    	job.setInputFormatClass(FFTInputFormat.class);
    	//job.setOutputFormatClass(FFTOutputFormat.class); // 傅利叶变换输出格式
    	job.setOutputFormatClass(AnalyseTextOutputFormat.class); // 傅利叶变换+特征提取输出格式
    	
    	System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}