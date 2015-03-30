/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package cz.cvut.fit.senkadam.mipdb.hbase.mapreduce;

import java.io.IOException;
import java.util.NavigableMap;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.TableMapReduceUtil;
import org.apache.hadoop.hbase.mapreduce.TableMapper;
import org.apache.hadoop.hbase.mapreduce.TableReducer;
import org.apache.hadoop.mapreduce.Job;

/**
 *
 * @author Adam Senk <senkadam@fit.cvut.cz>
 */
public class ActorAndDirector {

    static class Mapper extends TableMapper<ImmutableBytesWritable, ImmutableBytesWritable> {

        @Override
        public void map(ImmutableBytesWritable row, Result values, Mapper.Context context) throws IOException {
            //get the whole family ACTORS
            NavigableMap<byte[], byte[]> actorsMap = values.getFamilyMap("actors".getBytes());
            //iterate over actors family and emit ID of actor with "A" sign
            for (byte[] actor : actorsMap.values()) {
                try {
                    context.write(new ImmutableBytesWritable(actor), new ImmutableBytesWritable("A".getBytes()));
                } catch (InterruptedException ex) {
                    String msg = "Emiting in Map Phase";
                    org.apache.log4j.Logger.getLogger(ActorAndMovie.class.getName()).error(msg, ex);;
                }
            }
            //get the whole family DIRECTORS
            NavigableMap<byte[], byte[]> directorsMap = values.getFamilyMap("directors".getBytes());
            //iterate over actors family and emit ID of director with "D" sign
            //PLEASE NOTE, ID of director is not a value but a KEY of column
            for (byte[] director : directorsMap.descendingKeySet()) {
                try {
                    context.write(new ImmutableBytesWritable(director), new ImmutableBytesWritable("D".getBytes()));
                } catch (InterruptedException ex) {
                    String msg = "Emiting in Map Phase";
                    org.apache.log4j.Logger.getLogger(ActorAndMovie.class.getName()).error(msg, ex);;
                }
            }

        }
    }

    static class Reducer extends TableReducer<ImmutableBytesWritable, ImmutableBytesWritable, ImmutableBytesWritable> {

        @Override
        public void reduce(ImmutableBytesWritable key, Iterable<ImmutableBytesWritable> values, Reducer.Context context)
                throws IOException, InterruptedException {
            //ID actor
            String actor = new String(key.get());
            //set boolenas to false
            boolean actorIs = false;
            boolean directorIs = false;
            //iterate over the list of emited values;
            //if you find both "A" and "D" signs, then the person is both-
            //Actor and Director
            for (ImmutableBytesWritable v : values) {
                if (new String(v.get()).equals("A")) {
                    actorIs = true;
                }

                if (new String(v.get()).equals("D")) {
                    directorIs = true;
                }

                if (actorIs && directorIs) {
                    System.out.println("IS ACTOR AND DIRECTOR - "+actor);
                    break;
                }

            }

           
        }
    }

    public static void main(String[] args) {

        try {
            Configuration conf = HBaseConfiguration.create();

            Job job = new Job(conf);
            job.setJarByClass(ActorAndMovie.class);
           
            Scan scan = new Scan();
            scan.setCaching(2000);
            scan.setCacheBlocks(false);  // don't set to true for MR job

            TableMapReduceUtil.initTableMapperJob("movie", scan, Mapper.class, ImmutableBytesWritable.class,
                    ImmutableBytesWritable.class, job);
            TableMapReduceUtil.initTableReducerJob("movie", Reducer.class, job);

            job.waitForCompletion(true);
        } catch (Exception ex) {
            String msg = "Exception in Actor and Movie MR";
            org.apache.log4j.Logger.getLogger(ActorAndMovie.class.getName()).error(msg, ex);
        }

    }

}
