package Output;

import java.io.IOException;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.RecordWriter;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import HadoopFFT.FFTWritable;

public class FFTRecordWriter extends RecordWriter<NullWritable, FFTWritable> {
	private FSDataOutputStream out = null;
	
	public FFTRecordWriter(FSDataOutputStream fileOutput) {
		super();
		this.out = fileOutput;
	}

	@Override
	public void close(TaskAttemptContext job) throws IOException, InterruptedException {
		this.out.close();
	}

	@Override
	public void write(NullWritable key, FFTWritable value) throws IOException, InterruptedException {
		for (int count = 0; count < value.size(); count++) {
			this.out.writeInt(value.get(count).get());
		}
	}
}
