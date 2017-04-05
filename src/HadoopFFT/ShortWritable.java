package HadoopFFT;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import org.apache.hadoop.io.Writable;

public class ShortWritable implements Writable{
	private short number = 0;
	
	public ShortWritable() {
		// 构造空对象
	}

	public ShortWritable(short number) {
		this.number = number;
	}
	
	public void set(short number) {
		this.number = number;
	}
	
	public short getShort() {
		return(this.number);
	}

	@Override
	public void readFields(DataInput in) throws IOException {
		this.number = in.readShort();
	}

	@Override
	public void write(DataOutput out) throws IOException {
		out.writeShort(this.number);
	}
}

/*
public class ShortWritable implements WritableComparable<ShortWritable>{
	private short number = 0;
	
	public ShortWritable() {
		// 构造空对象
	}

	public ShortWritable(short number) {
		this.number = number;
	}
	
	@Override
	public void readFields(DataInput arg0) throws IOException {
		// TODO Auto-generated method stub
		
	}

	@Override
	public void write(DataOutput out) throws IOException {
		out.writeShort(this.number);
	}

	@Override
	public int compareTo(ShortWritable arg0) {
		return 0;
	}

}
*/
