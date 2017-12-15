//READING FROM HBASE OUTPUT TABLES
//WRITING RESULTS INTO HDFS (OUTPUTS DOWNLOADED TO DESKTOP AND SAVED AS CSV FILES)

import java.io.IOException;

import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.Reducer;;

public class ReducerHBaseReading extends
       Reducer<Text, IntWritable, Text, IntWritable>{
	
	private IntWritable result = new IntWritable();
	
	public void reduce(Text key, Iterable<IntWritable> values, Context context)
	       throws IOException, InterruptedException {
		
		int sum = 0;
		
		for (IntWritable val : values) {
			sum += val.get();
		}
		
		result.set(sum);
		
		context.write(key, result);
	}
	

}
