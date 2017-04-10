package HadoopFFT;

import java.io.IOException;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mapreduce.RecordWriter;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.fs.FileSystem;

public class ShortOutputFormat extends FileOutputFormat<LongWritable, ShortArrayWritable>{
	public RecordWriter<LongWritable, ShortArrayWritable> getRecordWriter(TaskAttemptContext job) throws IOException, InterruptedException {
		int partition = job.getTaskAttemptID().getTaskID().getId();
		Path outputDir = FileOutputFormat.getOutputPath(job);
		FileSystem fs = outputDir.getFileSystem(job.getConfiguration());
		Path file = new Path(outputDir + Path.SEPARATOR + job.getJobName().toString() + "_" + partition);
		FSDataOutputStream fileOut = fs.create(file, job);
		return new ShortRecordWriter(fileOut);
	}
}
