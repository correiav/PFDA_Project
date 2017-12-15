//READING FROM HBASE TABLES
//WRITING RESULTS INTO HDFS (OUTPUTS DOWNLOADED TO DESKTOP AND SAVED AS CSV FILES)
//BOTH TABLES IN HBASE ("yellow"/"cf1" and "green"/"cf1") HAD THE SAME COLUMN NAMES and FAMILY! THUS THIS CODE REFERS TO JOBS RUN FOR BOTH TABLES.

import java.io.IOException;

import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.TableMapper;
import org.apache.hadoop.io.*;


public class MapperHBaseReading extends 
	TableMapper<Text, IntWritable> {

        private Text outPassenger_Yellow = new Text();
        //private Text outPassengerDate_Yellow = new Text();
        //private Text outPassengerLocation_Yellow = new Text();
        //private Text outPassengerPayType_Yellow = new Text();
        //private Text outPassenger_Green = new Text();
        //private Text outPassengerDate_Green = new Text();
        //private Text outPassengerLocation_Green = new Text();
        //private Text outPassengerPayType_Green = new Text();
        
        private IntWritable ONE = new IntWritable(1);
        
        public void map(ImmutableBytesWritable row, Result columns, Context context)
        throws IOException, InterruptedException {
            
            String Passenger_Yellow = new String(columns.getValue("cf1".getBytes(), "passenger_count_output_yellow".getBytes()));
            outPassenger_Yellow.set(Passenger_Yellow);
            context.write(outPassenger_Yellow, ONE);
            
            //String PassengerDate_Yellow = new String(columns.getValue("cf1".getBytes(), "passenger_date_output_yellow".getBytes()));
            //outPassengerDate_Yellow.set(PassengerDate_Yellow);
            //context.write(outPassengerDate_Yellow, ONE);
            
            //String PassengerLocation_Yellow = new String(columns.getValue("cf1".getBytes(), "passenger_location_output_yellow".getBytes()));
            //outPassengerLocation_Yellow.set(PassengerLocation_Yellow);
            //context.write(outPassengerLocation_Yellow, ONE);
            
            //String PassengerPayType_Yellow = new String(columns.getValue("cf1".getBytes(), "passenger_paytype_output_yellow".getBytes()));
            //outPassengerPayType_Yellow.set(PassengerPayType_Yellow);
            //context.write(outPassengerPayType_Yellow, ONE);
            
            //String Passenger_Green = new String(columns.getValue("cf1".getBytes(), "passenger_count_output_green".getBytes()));
            //outPassenger_Green.set(Passenger_Green);
            //context.write(outPassenger_Green, ONE);
            
            //String PassengerDate_Green = new String(columns.getValue("cf1".getBytes(), "passenger_date_output_green".getBytes()));
            //outPassengerDate_Green.set(PassengerDate_Green);
            //context.write(outPassengerDate_Green, ONE);
            
            //String PassengerLocation_Green = new String(columns.getValue("cf1".getBytes(), "passenger_location_output_green".getBytes()));
            //outPassengerLocation_Green.set(PassengerLocation_Green);
            //context.write(PassengerLocation_Green, ONE);
            
            //String PassengerPayType_Green = new String(columns.getValue("cf1".getBytes(), "passenger_paytype_output_green".getBytes()))
            //outPassengerPayType_Green.set(PassengerPayType_Green);
            //context.write(outPassengerPayType_Green, ONE);
            
        }
    }

