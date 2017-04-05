package HadoopFFT;

import java.io.IOException;

import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.RecordReader;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;

public class WordRecordReader implements RecordReader<LongWritable, ShortArrayWritable>{
	JobConf jobConf = null;
	FileSplit fileSplit = null;
	long currentPosition = 0;
	FileSystem fileSystem = null;
	FSDataInputStream inputStream = null;
	ShortArrayWritable result = null;
	boolean processed = false;
	boolean finished = false;
	
	

	@Override
	public void close() throws IOException {
		try {
            if(inputStream != null)
                inputStream.close();
        } catch (IOException e) {
            e.printStackTrace();
        }
	}

	@Override
	public LongWritable createKey() {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public ShortArrayWritable createValue() {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public long getPos() throws IOException {
		return currentPosition;
	}

	@Override
	public float getProgress() throws IOException {
		return processed ? 1.0F : 0.0F;
	}

	@Override
	public boolean next(LongWritable key, ShortArrayWritable value)
			throws IOException {

		value.set(result);
		if (finished)
			return false;
		return true;
	}
/*
	private void processData(LongWritable key, ShortArrayWritable value) throws IOException {
		System.out.println(fileSplit.getPath().toString());
		
	}
	*/
}
