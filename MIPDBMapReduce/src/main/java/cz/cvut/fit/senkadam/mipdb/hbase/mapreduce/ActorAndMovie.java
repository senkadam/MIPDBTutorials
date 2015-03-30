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
public class ActorAndMovie {

    

    static class Mapper extends TableMapper<ImmutableBytesWritable, ImmutableBytesWritable> {
        
          
        @Override
        public void map(ImmutableBytesWritable row, Result values, Mapper.Context context) throws IOException {
            //geting name of the movie
            String movieName=new String(values.getValue("m_properties".getBytes(),
                    "m_name".getBytes()));
            //get the whole family ACTORS
            NavigableMap<byte[], byte[]> actorsMap=values.getFamilyMap("actors".getBytes());
            for(byte[] actor: actorsMap.values()){
                try {
                    context.write(new ImmutableBytesWritable(actor),new ImmutableBytesWritable(movieName.getBytes()));
                } catch (InterruptedException ex) {
                    String msg = "Emiting in Map Phase";
                    org.apache.log4j.Logger.getLogger(ActorAndMovie.class.getName()).error(msg,ex);;
                }
            }
            
        }
    }

    static class Reducer extends TableReducer<ImmutableBytesWritable, ImmutableBytesWritable, ImmutableBytesWritable> {

        @Override
        public void reduce(ImmutableBytesWritable key, Iterable<ImmutableBytesWritable> values, Reducer.Context context)
                throws IOException, InterruptedException {
            //ID actor
            String actor=new String(key.get());
            StringBuilder moviesBuilder=new StringBuilder(actor);
            moviesBuilder.append(" - ");
            //appending of movies names
            for(ImmutableBytesWritable v: values){
                moviesBuilder.append(new String(v.get()));
                moviesBuilder.append(", ");                        
            }
            
            moviesBuilder.delete(moviesBuilder.length()-2, moviesBuilder.length());
            //output
            System.out.println(moviesBuilder);
        }
    }

   

    public static void main(String[] args) {

        try {
            Configuration conf = HBaseConfiguration.create();
            
            
            Job job = new Job(conf);
            job.setJarByClass(ActorAndMovie.class);
            //DataInsert.getInstance().createTable("ActorAndMovie",);
                       
            
            Scan scan = new Scan();
            scan.setCaching(2000);
            scan.setCacheBlocks(false);  // don't set to true for MR job

            TableMapReduceUtil.initTableMapperJob("movie", scan, Mapper.class, ImmutableBytesWritable.class,
                    ImmutableBytesWritable.class, job);
            TableMapReduceUtil.initTableReducerJob("movie", Reducer.class, job);

            job.waitForCompletion(true);
        } catch (Exception ex) {
            String msg = "Exception in Actor and Movie MR";
            org.apache.log4j.Logger.getLogger(ActorAndMovie.class.getName()).error(msg,ex);
        }

    }

}
