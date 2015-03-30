/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package cz.cvut.fit.senkadam.mipdb.mapreduce;

import java.io.IOException;
import java.util.NavigableMap;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.TableMapReduceUtil;
import org.apache.hadoop.hbase.mapreduce.TableMapper;
import org.apache.hadoop.mapreduce.lib.output.NullOutputFormat;
import org.apache.hadoop.mapreduce.Job;

/**
 *
 * @author Adam Senk <senkadam@fit.cvut.cz>
 */
public class ActorAndDirectorInSameMovie {

    static class Mapper extends TableMapper<ImmutableBytesWritable, ImmutableBytesWritable> {

        @Override
        public void map(ImmutableBytesWritable row, Result values, Mapper.Context context) throws IOException {
            //get the whole family ACTORS
            NavigableMap<byte[], byte[]> actorsMap = values.getFamilyMap("actors".getBytes());
            //iterate over actors family 
            for (byte[] actor : actorsMap.values()) {
                String actorS=new String(actor);
                //get the whole family DIRECTORS
                NavigableMap<byte[], byte[]> directorsMap = values.getFamilyMap("directors".getBytes());
                //iterate over actors family and compare the IDs
                //PLEASE NOTE, ID of director is not a value but a KEY of column
                for (byte[] director : directorsMap.descendingKeySet()) {
                    String directorS=new String(director);
                    if(actorS.equals(directorS)){
                        String movieName=new String(values.getValue("m_properties".getBytes(),
                    "m_name".getBytes()));
                        System.out.println(actorS+" play in and directed movie "+movieName);
                    }
                }
            }

        }
    }


    public static void main(String[] args) {

        try {
            Configuration conf = HBaseConfiguration.create();

            Job job = new Job(conf);
            job.setJarByClass(ActorAndMovie.class);
            
            //Reduce phase is not needed
            job.setNumReduceTasks(0);
            job.setOutputFormatClass(NullOutputFormat.class);

            Scan scan = new Scan();
            scan.setCaching(2000);
            scan.setCacheBlocks(false);  // don't set to true for MR job

            TableMapReduceUtil.initTableMapperJob("movie", scan, Mapper.class, ImmutableBytesWritable.class,
                    ImmutableBytesWritable.class, job);
            

            job.waitForCompletion(true);
        } catch (Exception ex) {
            String msg = "Exception in Actor and Movie MR";
            org.apache.log4j.Logger.getLogger(ActorAndMovie.class.getName()).error(msg, ex);
        }

    }

}
