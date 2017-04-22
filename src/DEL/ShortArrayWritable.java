package DEL;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.ArrayWritable;

import HadoopFFT.HadoopFFT;

public class ShortArrayWritable extends ArrayWritable{
	private ShortWritable[] ARRAY = new ShortWritable[HadoopFFT.POINT];
	
	public ShortArrayWritable() {
		super(ArrayWritable.class);
	}
	
	public ShortArrayWritable(ShortWritable[] shorts) {
		super(ArrayWritable.class);
		this.ARRAY = shorts;
	}
	/*
	public ShortArrayWritable(short[] array) {
		super(ArrayWritable.class);
		this.shortArray = new ShortWritable[array.length];
		for (int count = 0; count < array.length; count++) {
			this.shortArray[count] = new ShortWritable(array[count]);
		}
	}
	*/
	public ShortWritable[] get() {
		return(this.ARRAY);
	}
	
	public Class<ShortArrayWritable> getValueClass() {
		return(ShortArrayWritable.class);
	}
	
	public void set(ShortWritable[] shorts) {
		this.ARRAY = shorts;
	}
	/*
	public void set(ShortArrayWritable shorts) {
		this.shortArray = new ShortWritable[shorts.getLength()];
		for (int count = 0; count < shorts.getLength(); count++) {
			this.shortArray[count] = shorts.getShortWritable(count);
		}
	}*/
	
	public void write(DataOutput out) {
		for (int count = 0; count < this.ARRAY.length; count++) {
			try {
				out.writeShort(ARRAY[count].get());
			} catch (IOException e) {
				e.printStackTrace();
			}
		}
	}
	
	public String[] toStrings() {
		String[] strings = new String[this.ARRAY.length];
		for (int count = 0; count < this.ARRAY.length; count++) {
			strings[count] = this.ARRAY[count].toString();
		}
		return strings;
	}
	
	public void readFields(DataInput in) throws IOException {
        for (int count = 0; count < this.ARRAY.length; count++) {
        	this.ARRAY[count] = new ShortWritable(in.readShort());
        }
        
	}
	
	public ShortWritable getShortWritable(int count) {
		return(this.ARRAY[count]);
	}
	
	public short getShortNumber(int count) {
		return(this.ARRAY[count].get());
	}
	
	public int getLength() {
		return(HadoopFFT.POINT);
	}
}
