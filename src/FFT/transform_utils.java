package FFT;

import org.apache.commons.math3.exception.DimensionMismatchException;
import org.apache.hadoop.io.IntWritable;
import HadoopFFT.FFTWritable;

public class transform_utils {
	/*
	 * 格式化输出快速傅利叶变换结果
	 * createComplexArray: 输出复数数组格式结果
	 * createFFTWritable: 输出FFTWritable结构结果
	 */
	public static Complex[] createComplexArray(int[][] dataRI) throws DimensionMismatchException{
	        if (dataRI.length != 2) {
	            throw new DimensionMismatchException(dataRI.length, 2);
	        }
	        int[] dataR = dataRI[0];
	        int[] dataI = dataRI[1];
	        if (dataR.length != dataI.length) {
	            throw new DimensionMismatchException(dataI.length, dataR.length);
	        }

	        int dataRLength = dataR.length;
	        Complex[] result = new Complex[dataRLength];
	        for (int count = 0; count < dataRLength; count++) {
	        	result[count] = new Complex(dataR[count], dataI[count]);
	        }
	        return result;
	    }
	
	public static FFTWritable createFFTWritable(int[][] dataRI) throws DimensionMismatchException {
        if (dataRI.length != 2) {
            throw new DimensionMismatchException(dataRI.length, 2);
        }
        int[] dataR = dataRI[0];
        int[] dataI = dataRI[1];
        if (dataR.length != dataI.length) {
            throw new DimensionMismatchException(dataI.length, dataR.length);
        }
        
        int dataRLength = dataR.length;
        FFTWritable result = new FFTWritable();
        for (int count = 0; count < dataRLength; count++) {
        	double tempR = dataR[count];
        	double tempI = dataI[count];
        	result.add(new IntWritable((int)(Math.sqrt(tempR * tempR + tempI * tempI))));
        }
        return result;
	}
}
