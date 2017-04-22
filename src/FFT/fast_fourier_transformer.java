package FFT;

import java.io.Serializable;
import org.apache.commons.math3.exception.DimensionMismatchException;
import org.apache.commons.math3.exception.MathIllegalArgumentException;
import org.apache.commons.math3.exception.MathIllegalStateException;
import org.apache.commons.math3.exception.util.LocalizedFormats;
import org.apache.commons.math3.transform.DftNormalization;
import org.apache.commons.math3.transform.TransformType;
import org.apache.commons.math3.util.ArithmeticUtils;
import org.apache.commons.math3.util.FastMath;
import HadoopFFT.FFTWritable;

public class fast_fourier_transformer implements Serializable {

	private static final long serialVersionUID = 1L;

	private final DftNormalization normalization;
	
	private static final double[] W_SUB_N_R =
        {  0x1.0p0, -0x1.0p0, 0x1.1a62633145c07p-54, 0x1.6a09e667f3bcdp-1
        , 0x1.d906bcf328d46p-1, 0x1.f6297cff75cbp-1, 0x1.fd88da3d12526p-1, 0x1.ff621e3796d7ep-1
        , 0x1.ffd886084cd0dp-1, 0x1.fff62169b92dbp-1, 0x1.fffd8858e8a92p-1, 0x1.ffff621621d02p-1
        , 0x1.ffffd88586ee6p-1, 0x1.fffff62161a34p-1, 0x1.fffffd8858675p-1, 0x1.ffffff621619cp-1
        , 0x1.ffffffd885867p-1, 0x1.fffffff62161ap-1, 0x1.fffffffd88586p-1, 0x1.ffffffff62162p-1
        , 0x1.ffffffffd8858p-1, 0x1.fffffffff6216p-1, 0x1.fffffffffd886p-1, 0x1.ffffffffff621p-1
        , 0x1.ffffffffffd88p-1, 0x1.fffffffffff62p-1, 0x1.fffffffffffd9p-1, 0x1.ffffffffffff6p-1
        , 0x1.ffffffffffffep-1, 0x1.fffffffffffffp-1, 0x1.0p0, 0x1.0p0
        , 0x1.0p0, 0x1.0p0, 0x1.0p0, 0x1.0p0
        , 0x1.0p0, 0x1.0p0, 0x1.0p0, 0x1.0p0
        , 0x1.0p0, 0x1.0p0, 0x1.0p0, 0x1.0p0
        , 0x1.0p0, 0x1.0p0, 0x1.0p0, 0x1.0p0
        , 0x1.0p0, 0x1.0p0, 0x1.0p0, 0x1.0p0
        , 0x1.0p0, 0x1.0p0, 0x1.0p0, 0x1.0p0
        , 0x1.0p0, 0x1.0p0, 0x1.0p0, 0x1.0p0
        , 0x1.0p0, 0x1.0p0, 0x1.0p0 };
	
	private static final double[] W_SUB_N_I =
        {  0x1.1a62633145c07p-52, -0x1.1a62633145c07p-53, -0x1.0p0, -0x1.6a09e667f3bccp-1
        , -0x1.87de2a6aea963p-2, -0x1.8f8b83c69a60ap-3, -0x1.917a6bc29b42cp-4, -0x1.91f65f10dd814p-5
        , -0x1.92155f7a3667ep-6, -0x1.921d1fcdec784p-7, -0x1.921f0fe670071p-8, -0x1.921f8becca4bap-9
        , -0x1.921faaee6472dp-10, -0x1.921fb2aecb36p-11, -0x1.921fb49ee4ea6p-12, -0x1.921fb51aeb57bp-13
        , -0x1.921fb539ecf31p-14, -0x1.921fb541ad59ep-15, -0x1.921fb5439d73ap-16, -0x1.921fb544197ap-17
        , -0x1.921fb544387bap-18, -0x1.921fb544403c1p-19, -0x1.921fb544422c2p-20, -0x1.921fb54442a83p-21
        , -0x1.921fb54442c73p-22, -0x1.921fb54442cefp-23, -0x1.921fb54442d0ep-24, -0x1.921fb54442d15p-25
        , -0x1.921fb54442d17p-26, -0x1.921fb54442d18p-27, -0x1.921fb54442d18p-28, -0x1.921fb54442d18p-29
        , -0x1.921fb54442d18p-30, -0x1.921fb54442d18p-31, -0x1.921fb54442d18p-32, -0x1.921fb54442d18p-33
        , -0x1.921fb54442d18p-34, -0x1.921fb54442d18p-35, -0x1.921fb54442d18p-36, -0x1.921fb54442d18p-37
        , -0x1.921fb54442d18p-38, -0x1.921fb54442d18p-39, -0x1.921fb54442d18p-40, -0x1.921fb54442d18p-41
        , -0x1.921fb54442d18p-42, -0x1.921fb54442d18p-43, -0x1.921fb54442d18p-44, -0x1.921fb54442d18p-45
        , -0x1.921fb54442d18p-46, -0x1.921fb54442d18p-47, -0x1.921fb54442d18p-48, -0x1.921fb54442d18p-49
        , -0x1.921fb54442d18p-50, -0x1.921fb54442d18p-51, -0x1.921fb54442d18p-52, -0x1.921fb54442d18p-53
        , -0x1.921fb54442d18p-54, -0x1.921fb54442d18p-55, -0x1.921fb54442d18p-56, -0x1.921fb54442d18p-57
        , -0x1.921fb54442d18p-58, -0x1.921fb54442d18p-59, -0x1.921fb54442d18p-60 };
	
    public fast_fourier_transformer(final DftNormalization normalization) {
        this.normalization = normalization;
    }
	
	public FFTWritable transform(final FFTWritable f, final TransformType type) {
		final int[][] dataRI = new int[2][f.size()];
		for (int count = 0; count < f.size(); count++)
			dataRI[0][count] = f.get(count).get(); // 将数据复制到2维数组中，dataRI[0]表示实数，dataRI[1]表示虚数
		
        transformInPlace(dataRI, normalization, type); // 变址操作

        return transform_utils.createFFTWritable(dataRI); // 格式化输出
    }

	public static void transformInPlace(final int[][] dataRI, final DftNormalization normalization, final TransformType type) {
        if (dataRI.length != 2) {
            throw new DimensionMismatchException(dataRI.length, 2);
        }
        final int[] dataR = dataRI[0];
        final int[] dataI = dataRI[1];
        if (dataR.length != dataI.length) {
            throw new DimensionMismatchException(dataI.length, dataR.length);
        }

        final int dataRILength = dataR.length;
        if (!ArithmeticUtils.isPowerOfTwo(dataRILength)) {
            throw new MathIllegalArgumentException(
                LocalizedFormats.NOT_POWER_OF_TWO_CONSIDER_PADDING,
                Integer.valueOf(dataRILength));
        }

        if (dataRILength == 1) {
            return;
        } else if (dataRILength == 2) {
            final int srcR0 = dataR[0];
            final int srcI0 = dataI[0];
            final int srcR1 = dataR[1];
            final int srcI1 = dataI[1];

            // X_0 = x_0 + x_1
            dataR[0] = srcR0 + srcR1;
            dataI[0] = srcI0 + srcI1;
            // X_1 = x_0 - x_1
            dataR[1] = srcR0 - srcR1;
            dataI[1] = srcI0 - srcI1;

            normalizeTransformedData(dataRI, normalization, type);
            return;
        }

        bitReversalShuffle2(dataR, dataI);

        // Do 4-term DFT.
        if (type == TransformType.INVERSE) {
            for (int i0 = 0; i0 < dataRILength; i0 += 4) {
                final int i1 = i0 + 1;
                final int i2 = i0 + 2;
                final int i3 = i0 + 3;

                final int srcR0 = dataR[i0];
                final int srcI0 = dataI[i0];
                final int srcR1 = dataR[i2];
                final int srcI1 = dataI[i2];
                final int srcR2 = dataR[i1];
                final int srcI2 = dataI[i1];
                final int srcR3 = dataR[i3];
                final int srcI3 = dataI[i3];

                // 4-term DFT
                // X_0 = x_0 + x_1 + x_2 + x_3
                dataR[i0] = srcR0 + srcR1 + srcR2 + srcR3;
                dataI[i0] = srcI0 + srcI1 + srcI2 + srcI3;
                // X_1 = x_0 - x_2 + j * (x_3 - x_1)
                dataR[i1] = srcR0 - srcR2 + (srcI3 - srcI1);
                dataI[i1] = srcI0 - srcI2 + (srcR1 - srcR3);
                // X_2 = x_0 - x_1 + x_2 - x_3
                dataR[i2] = srcR0 - srcR1 + srcR2 - srcR3;
                dataI[i2] = srcI0 - srcI1 + srcI2 - srcI3;
                // X_3 = x_0 - x_2 + j * (x_1 - x_3)
                dataR[i3] = srcR0 - srcR2 + (srcI1 - srcI3);
                dataI[i3] = srcI0 - srcI2 + (srcR3 - srcR1);
            }
        } else {
            for (int i0 = 0; i0 < dataRILength; i0 += 4) {
                final int i1 = i0 + 1;
                final int i2 = i0 + 2;
                final int i3 = i0 + 3;

                final int srcR0 = dataR[i0];
                final int srcI0 = dataI[i0];
                final int srcR1 = dataR[i2];
                final int srcI1 = dataI[i2];
                final int srcR2 = dataR[i1];
                final int srcI2 = dataI[i1];
                final int srcR3 = dataR[i3];
                final int srcI3 = dataI[i3];

                // 4-term DFT
                // X_0 = x_0 + x_1 + x_2 + x_3
                dataR[i0] = srcR0 + srcR1 + srcR2 + srcR3;
                dataI[i0] = srcI0 + srcI1 + srcI2 + srcI3;
                // X_1 = x_0 - x_2 + j * (x_3 - x_1)
                dataR[i1] = srcR0 - srcR2 + (srcI1 - srcI3);
                dataI[i1] = srcI0 - srcI2 + (srcR3 - srcR1);
                // X_2 = x_0 - x_1 + x_2 - x_3
                dataR[i2] = srcR0 - srcR1 + srcR2 - srcR3;
                dataI[i2] = srcI0 - srcI1 + srcI2 - srcI3;
                // X_3 = x_0 - x_2 + j * (x_1 - x_3)
                dataR[i3] = srcR0 - srcR2 + (srcI3 - srcI1);
                dataI[i3] = srcI0 - srcI2 + (srcR1 - srcR3);
            }
        }

        int lastN0 = 4;
        int lastLogN0 = 2;
        while (lastN0 < dataRILength) {
        	boolean LogTrueOrFalse = false;
            int n0 = lastN0 << 1;
            int logN0 = lastLogN0 + 1;
            double wSubN0R = W_SUB_N_R[logN0];
            double wSubN0I = W_SUB_N_I[logN0];
            if (type == TransformType.INVERSE) {
                wSubN0I = -wSubN0I;
            }

            // Combine even/odd transforms of size lastN0 into a transform of size N0 (lastN0 * 2).
            for (int destEvenStartIndex = 0; destEvenStartIndex < dataRILength; destEvenStartIndex += n0) {
                int destOddStartIndex = destEvenStartIndex + lastN0;

                double wSubN0ToRR = 1;
                double wSubN0ToRI = 0;

                for (int r = 0; r < lastN0; r++) {
                	
                    double grR = dataR[destEvenStartIndex + r];
                    double grI = dataI[destEvenStartIndex + r];
                    double hrR = dataR[destOddStartIndex + r];
                    double hrI = dataI[destOddStartIndex + r];

                    // dest[destEvenStartIndex + r] = Gr + WsubN0ToR * Hr
                    dataR[destEvenStartIndex + r] = (int)(grR + wSubN0ToRR * hrR - wSubN0ToRI * hrI);
                    dataI[destEvenStartIndex + r] = (int)(grI + wSubN0ToRR * hrI + wSubN0ToRI * hrR);
                    // dest[destOddStartIndex + r] = Gr - WsubN0ToR * Hr
                    dataR[destOddStartIndex + r] = (int)(grR - (wSubN0ToRR * hrR - wSubN0ToRI * hrI));
                    dataI[destOddStartIndex + r] = (int)(grI - (wSubN0ToRR * hrI + wSubN0ToRI * hrR));
                    
                    if (dataR[destEvenStartIndex + r] > 1000000000 | dataI[destEvenStartIndex + r] > 1000000000 | 
                    		dataR[destOddStartIndex + r] > 1000000000 | dataI[destOddStartIndex + r] > 1000000000) {
                    	System.out.println("dataR[destEvenStartIndex + r] = " + dataR[destEvenStartIndex + r] + 
                    			" Log = " + (int)Math.log(dataR[destEvenStartIndex + r]));
                    	System.out.println("dataI[destEvenStartIndex + r] = " + dataI[destEvenStartIndex + r] + 
                    			" Log = " + (int)Math.log(dataI[destEvenStartIndex + r]));
                    	System.out.println("dataR[destOddStartIndex + r] = " + dataR[destOddStartIndex + r] + 
                    			" Log = " + (int)Math.log(dataR[destOddStartIndex + r]));
                    	System.out.println("dataI[destOddStartIndex + r] = " + dataI[destOddStartIndex + r] + 
                    			" Log = " + (int)Math.log(dataI[destOddStartIndex + r]));
                    	LogTrueOrFalse = true;
                    }

                    // WsubN0ToR *= WsubN0R
                    double nextWsubN0ToRR = wSubN0ToRR * wSubN0R - wSubN0ToRI * wSubN0I;
                    double nextWsubN0ToRI = wSubN0ToRR * wSubN0I + wSubN0ToRI * wSubN0R;
                    wSubN0ToRR = nextWsubN0ToRR;
                    wSubN0ToRI = nextWsubN0ToRI;
                }
            }
            
            if (LogTrueOrFalse) {
            	for (int count = 0; count < dataR.length; count++) {
            		dataR[count] = (int)Math.log(dataR[count]);
            		dataI[count] = (int)Math.log(dataI[count]);
            	}
            	LogTrueOrFalse = false;
            }
            
            lastN0 = n0;
            lastLogN0 = logN0;
        }

        normalizeTransformedData(dataRI, normalization, type);
	}

	private static void bitReversalShuffle2(int[] a, int[] b) {
		/*
		 * 变址操作：傅利叶变换后，频域数据会倒位序，需要变换时域数据的顺序，保证变换后频域数据顺序正常。
		 * 变址操作——雷德(Rader)算法，请参考程佩青老师的《数字信号处理教程》P188-P190。
		 */
        final int n = a.length;
        assert b.length == n;
        final int halfOfN = n >> 1;

        int j = 0;
        for (int i = 0; i < n; i++) {
            if (i < j) {
                // swap indices i & j
            	int temp = a[i];
                a[i] = a[j];
                a[j] = temp;

                temp = b[i];
                b[i] = b[j];
                b[j] = temp;
            }

            int k = halfOfN;
            while (k <= j && k > 0) {
                j -= k;
                k >>= 1;
            }
            j += k;
        }
    }
	
	private static void normalizeTransformedData(final int[][] dataRI, final DftNormalization normalization, final TransformType type) {
        final int[] dataR = dataRI[0];
        final int[] dataI = dataRI[1];
        final int n = dataR.length;
        assert dataI.length == n;

        switch (normalization) {
            case STANDARD:
                if (type == TransformType.INVERSE) {
                    final double scaleFactor = 1.0 / ((double) n);
                    for (int i = 0; i < n; i++) {
                        dataR[i] *= scaleFactor;
                        dataI[i] *= scaleFactor;
                    }
                }
                break;
            case UNITARY:
                final double scaleFactor = 1.0 / FastMath.sqrt(n);
                for (int i = 0; i < n; i++) {
                    dataR[i] *= scaleFactor;
                    dataI[i] *= scaleFactor;
                }
                break;
            default:
                /*
                 * This should never occur in normal conditions. However this
                 * clause has been added as a safeguard if other types of
                 * normalizations are ever implemented, and the corresponding
                 * test is forgotten in the present switch.
                 */
                throw new MathIllegalStateException();
        }
	}
}