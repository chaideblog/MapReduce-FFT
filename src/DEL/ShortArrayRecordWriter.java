package DEL;

import java.io.IOException;

import org.apache.hadoop.fs.FSDataOutputStream;
//import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.RecordWriter;
import org.apache.hadoop.mapreduce.TaskAttemptContext;

public class ShortArrayRecordWriter extends RecordWriter<NullWritable, ShortArrayWritable> {
	private FSDataOutputStream out;
	
	public ShortArrayRecordWriter(FSDataOutputStream fileOutput) {
		super();
		this.out = fileOutput;
	}

	@Override
	public void close(TaskAttemptContext job) throws IOException, InterruptedException {
		this.out.close();
		System.out.println("ShortArrayRecordWriter.close: Close output stream!");
	}

	@Override
	public void write(NullWritable key, ShortArrayWritable value) throws IOException, InterruptedException {
		for (int count = 0; count < value.getLength(); count++) {
			this.out.writeShort(value.getShortNumber(count));
		}
		//System.out.println("ShortArrayRecordWriter.write: Write output stream!");
	}

}
