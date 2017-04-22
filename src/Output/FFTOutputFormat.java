package Output;

import java.io.IOException;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import HadoopFFT.FFTWritable;

public class FFTOutputFormat extends FileOutputFormat<NullWritable, FFTWritable>{
	/*
	 * 输出傅利叶变换的结果
	 */
	public FFTRecordWriter getRecordWriter(TaskAttemptContext job) throws IOException, InterruptedException {
		int partition = job.getTaskAttemptID().getTaskID().getId();
		Path outputDir = FileOutputFormat.getOutputPath(job);
		FileSystem fs = outputDir.getFileSystem(job.getConfiguration());
		Path file = new Path(outputDir + Path.SEPARATOR + job.getJobName().toString() + "_" + partition);
		FSDataOutputStream fileOut = fs.create(file, job);
		return new FFTRecordWriter(fileOut);
	}
}
