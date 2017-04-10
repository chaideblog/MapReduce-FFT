package HadoopFFT;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.ArrayWritable;

public class ShortArrayWritable extends ArrayWritable{
	private ShortWritable[] shortArray = null;

	public ShortArrayWritable() {
		super(ArrayWritable.class);
	}
	
	public ShortArrayWritable(short[] array) {
		super(ArrayWritable.class);
		this.shortArray = new ShortWritable[array.length];
		for (int count = 0; count < array.length; count++) {
			this.shortArray[count] = new ShortWritable(array[count]);
		}
	}
	
	public ShortWritable[] get() {
		return(this.shortArray);
	}
	
	public void set(ShortArrayWritable shorts) {
		this.shortArray = new ShortWritable[shorts.getLength()];
		for (int count = 0; count < shorts.getLength(); count++) {
			this.shortArray[count] = shorts.getShortWritable(count);
		}
	}
	
	public void write(DataOutput out) {
		for (int count = 0; count < this.shortArray.length; count++) {
			try {
				out.writeShort(shortArray[count].getShort());
			} catch (IOException e) {
				e.printStackTrace();
			}
		}
	}
	
	public String[] toStrings() {
		String[] strings = new String[this.shortArray.length];
		for (int count = 0; count < this.shortArray.length; count++) {
			strings[count] = this.shortArray[count].toString();
		}
		return strings;
	}
	
	public void readFields(DataInput in) throws IOException {
		this.shortArray = new ShortWritable[HadoopFFT.POINT];
        for (int count = 0; count < this.shortArray.length; count++) {
        	this.shortArray[count] = new ShortWritable(in.readShort());
        }
        
	}
	
	public int getLength() {
		return(shortArray.length);
	}
	
	public ShortWritable getShortWritable(int count) {
		return(this.shortArray[count]);
	}
	
	public short getShortNumber(int count) {
		return(this.shortArray[count].getShort());
	}
}
