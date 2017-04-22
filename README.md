# MapReduce-FFT

本程序通过Hadoop实现大规模导航信号的快速傅立叶变换及特征提取。

## 程序结构

	MapReduce-FFT
		|
		|--bin(字节码文件)
		|
		|--src
				|
				|--HadoopFFT
				|			|
				|			|--HadoooFFT.java(主类)
				|			|
				|			|--FFTWritable.java(自定义数据结构)
				|			|
				|			|--commons-math3-3.6.1.jar(Apache开发的数学包)
				|
				|--FFT
				|		|
				|		|--fast_fourier_transformer.java(快速傅立叶变换)
				|		|
				|		|--transform_utils.java(格式化输出)
				|		|
				|		|--Complex.java(复数数据结构)
				|
				|--Input
				|		|
				|		|--FFTInputFormat.java(输入分片及读取)
				|		|
				|		|--FFTRecordReader.java(读取分片内容)
				|
				|--Output
							|
							|--FFTOutputFormat.java(傅立叶变换结果输出)
							|
							|--FFTRecordWriter.java(控制输出傅立叶变换结果)
							|
							|--AnalyseTextOutputFormat.java(特征提取结果输出)
							|
							|--AnalyseTextRecordWriter.java(控制输出特征提取结果)
