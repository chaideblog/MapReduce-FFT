package HadoopFFT;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import org.apache.hadoop.io.IntWritable;

public class FFTWritable extends IntWritable{
	/*
	 * FFTWritable继承IntWritable类，FFTWritable类保存IntWritable列表。
	 * 任务读入的采样点数据，保存在IntWritable列表中，傅利叶变换也使用该类。
	 */
	private List<IntWritable> IntWritableList = new ArrayList<IntWritable>();

	public FFTWritable() {
	}
	
	public FFTWritable(List<IntWritable> list) {
		// 初始化
		this.IntWritableList = list;
	}

	public void readFields(DataInput in) throws IOException {
		// 反序列化
		List<IntWritable> temp = new ArrayList<IntWritable>();
		for (int count = 0; count < HadoopFFT.POINT; count++) {
			temp.add(new IntWritable(in.readInt()));
        }
		this.IntWritableList = temp;
	}

	@Override
	public void write(DataOutput out) throws IOException {
		// 序列化
		for (int count = 0; count < this.IntWritableList.size(); count++) {
			try {
				out.writeInt(IntWritableList.get(count).get());
			} catch (IOException e) {
				e.printStackTrace();
			}
		}
	}
	
	public IntWritable get(int count) {
		// 获取count位置的数据
		return(this.IntWritableList.get(count));
	}
	
	public int size() {
		// 获取列表的大小
		return(this.IntWritableList.size());
	}
	
	public void set(List<IntWritable> list) {
		// 设置列表
		this.IntWritableList = list;
	}
	
	public void add(IntWritable temp) {
		// 给列表添加内容
		this.IntWritableList.add(temp);
	}
}
