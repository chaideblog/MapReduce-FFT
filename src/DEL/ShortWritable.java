package DEL;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import org.apache.hadoop.io.Writable;

public class ShortWritable implements Writable{
	private short NUMBER = 0;
	
	public ShortWritable() {
		// 构造空对象
	}

	public ShortWritable(short number) {
		this.NUMBER = number;
	}
	
	public void set(short number) {
		this.NUMBER = number;
	}
	
	public short get() {
		return(this.NUMBER);
	}

	@Override
	public void readFields(DataInput in) throws IOException {
		this.NUMBER = in.readShort();
	}

	@Override
	public void write(DataOutput out) throws IOException {
		out.writeShort(this.NUMBER);
	}
}

