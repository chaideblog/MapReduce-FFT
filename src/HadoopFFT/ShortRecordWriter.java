package HadoopFFT;

import java.io.IOException;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mapreduce.RecordWriter;
import org.apache.hadoop.mapreduce.TaskAttemptContext;

public class ShortRecordWriter extends RecordWriter<LongWritable, ShortArrayWritable> {
	private FSDataOutputStream out;
	
	public ShortRecordWriter(FSDataOutputStream fileOutput) {
		super();
		this.out = fileOutput;
	}

	@Override
	public void close(TaskAttemptContext arg0) throws IOException,
			InterruptedException {
		out.close();
	}

	@Override
	public void write(LongWritable key, ShortArrayWritable value) throws IOException, InterruptedException {
		for (int count = 0; count < value.getLength(); count++) {
			out.writeShort(value.getShortNumber(count));
		}
	}

}
