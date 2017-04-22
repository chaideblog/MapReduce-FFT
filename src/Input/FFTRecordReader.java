package Input;

import java.io.IOException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import HadoopFFT.FFTWritable;
import HadoopFFT.HadoopFFT;

public class FFTRecordReader extends RecordReader<LongWritable, FFTWritable>{
	private FSDataInputStream inputStream = null;
    private long start, end, pos;
    private Configuration conf = null;
    private FileSplit fileSplit = null;
    private LongWritable key = null;
    private FFTWritable value = null;
    private boolean processed = false;
    
    public FFTRecordReader() throws IOException {
    }

    public void close() {
    	// 关闭文件流
        try {
            if(inputStream != null) {
                inputStream.close();
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    public float getProgress() {
    	// 获取处理进度
        return ((processed == true)? 1.0f : 0.0f);
    }
  
    public LongWritable getCurrentKey() throws IOException, InterruptedException {
    	// 获取当前的Key
        return this.key;
    }
  
    public FFTWritable getCurrentValue() throws IOException, InterruptedException {
        // 获取当前的Value
        return this.value;
    }
 
    public void initialize(InputSplit inputSplit, TaskAttemptContext context) throws IOException, InterruptedException {
    	// 进行初始化工作，打开文件流，根据分块信息设置起始位置和长度等等
        fileSplit = (FileSplit)inputSplit;
        conf = context.getConfiguration();
  
        this.start = fileSplit.getStart();
        this.end = this.start + fileSplit.getLength();
        System.out.println("读取文件的起始字节：" + this.start + "\t读取文件的终止字节：" + this.end);
        try{
            Path path = fileSplit.getPath();
            FileSystem fs = path.getFileSystem(conf);
            this.inputStream = fs.open(path);
            this.inputStream.seek(this.start);
            this.pos = this.start;
        } catch(IOException e) {
            e.printStackTrace();
        }
    }
  
    public boolean nextKeyValue() throws IOException, InterruptedException {
        // 生成下一个键值对
        if(this.pos < this.end) {
        	key = new LongWritable(this.pos);
        	value = new FFTWritable();
            for (int count = 0; count < HadoopFFT.POINT; count++) {
            	value.add(new IntWritable((int)inputStream.readShort()));
            }
            this.pos = inputStream.getPos();
            return true;
        } else {
            processed = true;
            return false;
        }
    }
}
