//READING FROM HBASE OUTPUT TABLES
//WRITING RESULTS INTO HDFS (OUTPUTS DOWNLOADED TO DESKTOP AND SAVED AS CSV FILES)

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.mapreduce.TableMapReduceUtil;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;


public class DriverHbaseReading {

	public static void main(String[] args) throws Exception {
		Configuration conf = HBaseConfiguration.create();
		String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
		Scan scan = new Scan();
		Job job = Job.getInstance(conf, "MapReduce Count Patterns");
		job.setJarByClass(DriverHbaseReading.class);
		TableMapReduceUtil.initTableMapperJob("passenger_count_output_yellow", scan,
                                              //"passenger_date_output_yellow", scan,
                                              //"passenger_location_output_yellow", scan,
                                              //"passenger_paytype_output_yellow", scan,
                                              //"passenger_count_output_green", scan,
                                              //"passenger_date_output_green", scan,
                                              //"passenger_location_output_green", scan,
                                              //"passenger_paytype_output_green", scan,
				                              MapperHBaseReading.class,
				                              Text.class, IntWritable.class, 
				                              job);
		job.setReducerClass(ReducerHBaseReading.class);
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(IntWritable.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(IntWritable.class);
		FileOutputFormat.setOutputPath(job, new Path(path/to/file));
		System.exit(job.waitForCompletion(true) ? 0 : 1);
		}
}
