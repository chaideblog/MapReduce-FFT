package HadoopFFT;

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
	
	public void set(ShortArrayWritable shorts) {
		this.shortArray = new ShortWritable[shorts.getLength()];
		for (int count = 0; count < shorts.getLength(); count++) {
			this.shortArray[count] = shorts.getShortWritable(count);
		}
	}
	
	public ShortWritable[] get() {
		return(this.shortArray);
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
