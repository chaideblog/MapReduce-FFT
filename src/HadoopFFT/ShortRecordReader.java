package HadoopFFT;

import java.io.IOException;  
import org.apache.hadoop.fs.FSDataInputStream;  
import org.apache.hadoop.fs.FileSystem;  
import org.apache.hadoop.fs.Path;  
import org.apache.hadoop.io.LongWritable;  
import org.apache.hadoop.mapreduce.TaskAttemptContext;  
import org.apache.hadoop.mapreduce.lib.input.FileSplit;  
import org.apache.hadoop.mapreduce.InputSplit;  
import org.apache.hadoop.conf.Configuration;  
import org.apache.hadoop.mapreduce.RecordReader;
import HadoopFFT.HadoopFFT;

public class ShortRecordReader extends RecordReader<LongWritable, ShortArrayWritable>{
	private FSDataInputStream inputStream = null;  
    private long start,end,pos;  
    private Configuration conf = null;  
    private FileSplit fileSplit = null;  
    private LongWritable key = null;  
    private ShortArrayWritable value = null;  
    private boolean processed = false;  
    
    private short[] shortArray = null;
    
    public ShortRecordReader() throws IOException {  
    }  
  
    public void close() {
    	// 关闭文件流
        try {  
            if(inputStream != null)  
                inputStream.close();  
        } catch (IOException e) {  
            e.printStackTrace();  
        }  
    }  
  
    public float getProgress() {
    	// 获取处理进度 
        return ((processed == true)? 1.0f : 0.0f);  
    }  
  
    public LongWritable getCurrentKey() throws IOException,  
    InterruptedException {
    	// 获取当前的Key 
        return key;  
    }  
  
    public ShortArrayWritable getCurrentValue() throws IOException,InterruptedException {  
        // 获取当前的Value 
        return value;  
    }  
 
    public void initialize(InputSplit inputSplit, TaskAttemptContext context)
            throws IOException, InterruptedException {
    	// 进行初始化工作，打开文件流，根据分块信息设置起始位置和长度等等 
        fileSplit = (FileSplit)inputSplit;  
        conf = context.getConfiguration();  
  
        this.start = fileSplit.getStart();  
        this.end = this.start + fileSplit.getLength();  
  
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
        	key = new LongWritable();
            key.set(this.pos);
            //System.out.println(this.pos);
            this.shortArray = new short[HadoopFFT.POINT];
            for (int count = 0; count < this.shortArray.length; count++) {
            	this.shortArray[count] = inputStream.readShort();
            }
            value = new ShortArrayWritable(this.shortArray);
            // value.set(new ShortArrayWritable(this.shortArray));
            this.pos = inputStream.getPos();
            return true;
        } else {  
            processed = true;  
            return false;  
        }
    }
}
