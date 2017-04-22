package DEL;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.BlockLocation;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;

public class ShortArrayInputFormat extends FileInputFormat<LongWritable, ShortArrayWritable> {
	private static final double SPLIT_SLOP = 1.1;
	
	protected boolean isSplitable(Configuration conf, Path path) {
		return true;
	}
	
	public List<InputSplit> getSplits(JobContext job) throws IOException {
		long minSplitSize = Math.max(getFormatMinSplitSize(), getMinSplitSize(job));// 最小分片大小
		long maxSplitSize = getMaxSplitSize(job);// 最大分片大小

        List<InputSplit> splits = new ArrayList<InputSplit>();// splits链表用来储存计算得到的分片结果
        List<FileStatus> files = listStatus(job);
        
        for (FileStatus file : files) {
        	Path path = file.getPath();
        	FileSystem fs = path.getFileSystem(job.getConfiguration());
        	long length = file.getLen();
        	BlockLocation[] blkLocations = fs.getFileBlockLocations(file, 0, length);// 获取该文件所有block的信息列表
        	if ((length != 0) && isSplitable(job, path)) {
        		// 判断文件是否可分割，通常是可分割的，但文件是压缩的，将不可分割
        		// 是否分割由FileInputFormat的isSplitable控制
        		long blockSize = file.getBlockSize();
        		long splitSize = Math.max(minSplitSize, Math.min(maxSplitSize, blockSize));// 计算分片大小
        		
        		long bytesRemaining = length;
        		while (((double) bytesRemaining) / splitSize > SPLIT_SLOP) {
        			// 循环分片，当剩余数据与分片大小比值小于SPLIT_SLOP，停止分片
        			int blkIndex = getBlockIndex(blkLocations, length - bytesRemaining);
        			splits.add(new FileSplit(path, length - bytesRemaining, splitSize, blkLocations[blkIndex].getHosts()));
        			bytesRemaining -= splitSize;
        		}
        		if (bytesRemaining != 0) {
        			// 处理剩下的数据
        			splits.add(new FileSplit(path, length - bytesRemaining, bytesRemaining, blkLocations[blkLocations.length - 1].getHosts()));
        		} else if (length != 0) {
        			// 不可以split，整块返回
        			splits.add(new FileSplit(path, 0, length, blkLocations[0].getHosts()));
        		} else {
        			// 对于长度为0的文件，创建空Hosts列表，返回
        			splits.add(new FileSplit(path, 0, length, new String[0]));
        		}
        	}
        }
        // job.getConfiguration().setLong(NUM_INPUT_FILES, files.size()); // 设置输入文件数量
    	return splits;
    }
	
	@Override
	public RecordReader<LongWritable, ShortArrayWritable> createRecordReader(InputSplit split, TaskAttemptContext context) throws IOException, InterruptedException {
		ShortArrayRecordReader reader = new ShortArrayRecordReader();
		reader.initialize(split, context);
		return reader;
	}
	
}

