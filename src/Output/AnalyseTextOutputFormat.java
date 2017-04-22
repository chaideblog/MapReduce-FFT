package Output;

import java.io.IOException;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class AnalyseTextOutputFormat extends FileOutputFormat<NullWritable, Text>{
	/*
	 * 输出特征提取的结果
	 */
	public AnalyseTextRecordWriter getRecordWriter(TaskAttemptContext job) throws IOException, InterruptedException {
		int partition = job.getTaskAttemptID().getTaskID().getId();
		Path outputDir = FileOutputFormat.getOutputPath(job);
		FileSystem fs = outputDir.getFileSystem(job.getConfiguration());
		Path file = new Path(outputDir + Path.SEPARATOR + job.getJobName().toString() + "_" + partition);
		FSDataOutputStream fileOut = fs.create(file, job);
		return new AnalyseTextRecordWriter(fileOut);
	}
}
