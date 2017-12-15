//READING FROM HBASE OUTPUT TABLES
//WRITING RESULTS INTO HDFS (OUTPUTS DOWNLOADED TO DESKTOP AND SAVED AS CSV FILES)
//BOTH TABLES IN HBASE ("yellow"/"cf1" and "green"/"cf1") HAD THE SAME COLUMN NAMES and FAMILY! THUS THIS CODE REFERS TO JOBS RUN FOR BOTH TABLES.

import java.io.IOException;

import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.TableMapper;
import org.apache.hadoop.io.*;


public class MapperHBaseReading extends 
	TableMapper<Text, IntWritable> {

	private Text outPassenger = new Text(); //1
    //private Text outPassengerDate = new Text(); //2
    //private Text outPassengerLocation = new Text(); //3
    //private Text outPayType = new Text(); //4

    private IntWritable ONE = new IntWritable(1);

	public void map(ImmutableBytesWritable row, Result columns, Context context)
    	throws IOException, InterruptedException {
	
            String Passenger = new String(columns.getValue("cf1".getBytes(), "passenger_count".getBytes()));
            String PickupDate = new String(columns.getValue("cf1".getBytes(), "pickup_date_time".getBytes()));
            String DropoffDate = new String(columns.getValue("cf1".getBytes(), "dropoff_date_time".getBytes()));
            String PickupLocation = new String(columns.getValue("cf1".getBytes(), "pu_location_id".getBytes()));
            String DropoffLocation = new String(columns.getValue("cf1".getBytes(), "do_location_id".getBytes()));
		    String PaymentType = new String(columns.getValue("cf1".getBytes(), "payment_type".getBytes()));
        
            //Next line removes the hour (e.g. 00:00) from the timestamp
            //String strNewPickupDate = PickupDate.substring(0,10);
            //String strNewDropoffDate = DropoffDate.substring(0, 10);
            
        // Mappper Jobs
        // Note that only the mapper side needed major modifications because. Also some jobs required string concatenation.
            
        // 1 Count the Passengers
		outPassenger.set("passenger " + Passenger);
		context.write(outPassenger, ONE);
            
        // 2 Count the PickupDate trips without the hours grouping by passenger.
        //outPassengerDate.set("passenger " + Passenger + " pickup date " + strNewPickupDate);
        //context.write(outPassengerDate, ONE);
            
        // 3 Count the pickup and dropoff locations grouping by passenger
        //outPassengerLocation.set("passenger " + Passenger + " pickup " + PickLocation + " dropoff " + DropLocation);
        //context.write(outPassengerLocation, ONE);
            
        //4 Count the payment type grouping by passenger
        //outPassengerPayType.set("passenger " + Passenger + " payment type " + PaymentType);
        //context.write(outPassengerPayType, ONE);
            
	}
}
