//READING FROM HBASE TABLES ("yellow" and "green")
//WRITING RESULTS INTO NEW HBASE TABLES
//Note that the Driver required modifications because the data is being read from HBase tables and the results written in new HBase tables named as follows.

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.mapreduce.TableMapReduceUtil;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.util.GenericOptionsParser;


public class DriverHbaseWriting {
	public static void main(String[] args) throws Exception {
		Configuration conf = HBaseConfiguration.create();
		String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
		Scan scan = new Scan();
		Job job = Job.getInstance(conf, "MapReduce Count Patterns");
		job.setJarByClass(MapperHBaseWriting.class);
		TableMapReduceUtil.initTableMapperJob("yellow", scan,
                                              //"green", scan,
				                              MapperHBaseWriting.class,
				                              Text.class, IntWritable.class, 
				                              job);
		TableMapReduceUtil.initTableReducerJob("passenger_count_output_yellow", ReducerHBaseWriting.class, job);
		job.setNumReduceTasks(1);
        
        //TableMapReduceUtil.initTableReducerJob("passenger_date_output_yellow", ReducerHBaseWriting.class, job);
        //job.setNumReduceTasks(1);
        
        //TableMapReduceUtil.initTableReducerJob("passenger_location_output_yellow", ReducerHBaseWriting.class, job);
        //job.setNumReduceTasks(1);
        
        //TableMapReduceUtil.initTableReducerJob("passenger_paytype_output_yellow", ReducerHBaseWriting.class, job);
        //job.setNumReduceTasks(1);
        
        //TableMapReduceUtil.initTableReducerJob("passenger_count_output_green", ReducerHBaseWriting.class, job);
        //job.setNumReduceTasks(1);
        
        //TableMapReduceUtil.initTableReducerJob("passenger_date_output_green", ReducerHBaseWriting.class, job);
        //job.setNumReduceTasks(1);
        
        //TableMapReduceUtil.initTableReducerJob("passenger_location_output_green", ReducerHBaseWriting.class, job);
        //job.setNumReduceTasks(1);
        
        //TableMapReduceUtil.initTableReducerJob("passenger_paytype_output_green", ReducerHBaseWriting.class, job);
        //job.setNumReduceTasks(1);
        
		System.exit(job.waitForCompletion(true) ? 0 : 1);
		}
}


