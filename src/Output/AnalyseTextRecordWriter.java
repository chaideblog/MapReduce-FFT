package Output;

import java.io.IOException;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.RecordWriter;
import org.apache.hadoop.mapreduce.TaskAttemptContext;

public class AnalyseTextRecordWriter extends RecordWriter<NullWritable, Text> {
	private FSDataOutputStream out = null;
	
	public AnalyseTextRecordWriter(FSDataOutputStream fileOutput) {
		super();
		this.out = fileOutput;
	}

	@Override
	public void close(TaskAttemptContext job) throws IOException, InterruptedException {
		this.out.close();
	}

	@Override
	public void write(NullWritable key, Text value) throws IOException, InterruptedException {
		//System.out.println(value.toString());
		this.out.writeBytes(value.toString());
	}
}
