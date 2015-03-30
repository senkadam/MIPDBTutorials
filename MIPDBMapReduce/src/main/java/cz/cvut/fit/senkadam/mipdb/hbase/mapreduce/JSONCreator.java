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
import org.apache.hadoop.mapreduce.lib.output.NullOutputFormat;
import org.apache.hadoop.mapreduce.Job;

/**
 *
 * @author Adam Senk <senkadam@fit.cvut.cz>
 */
public class JSONCreator {

    static class Mapper extends TableMapper<ImmutableBytesWritable, ImmutableBytesWritable> {

        @Override
        public void map(ImmutableBytesWritable row, Result values, Mapper.Context context) throws IOException {
            
            StringBuilder json=new StringBuilder("{\"movie\":{");
            //ID
            json.append("\"id\":");
            json.append("\"");
            json.append(new String(row.get()));
            json.append("\"").append(",");
            //name
            json.append("\"m_name\":");
            json.append("\"");
            json.append(new String(values.getValue("m_properties".getBytes(),
                    "m_name".getBytes())));
            json.append("\"").append(",");
            //first_night
            json.append("\"first_night\":");
            json.append("\"");
            json.append(new String(values.getValue("m_properties".getBytes(),
                    "first_night".getBytes())));
            json.append("\"").append(",");
            //genre
            json.append("\"genre\":");
            json.append("\"");
            json.append(new String(values.getValue("m_properties".getBytes(),
                    "genre".getBytes())));
            json.append("\"").append(",");
            //runtime
            json.append("\"runtime\":");
            json.append("\"");
            json.append(new String(values.getValue("m_properties".getBytes(),
                    "runtime".getBytes())));
            json.append("\"").append(",");
            
            //actors
            json.append("\"roles\":[");
            
            NavigableMap<byte[], byte[]> actorsMap = values.getFamilyMap("actors".getBytes());
            //iterate over actors family and construct JSON
            for (byte[] role : actorsMap.descendingKeySet()) {
                json.append("{");
                json.append("\"role\":");
                json.append("\"").append(new String(role)).append("\",");
                json.append("\"actor\":");
                json.append(new String(actorsMap.get(role)));
                json.append("},");               
            }
            json.deleteCharAt(json.lastIndexOf(","));
            json.append("],");
            
            json.append("\"directors\":[");
            
            NavigableMap<byte[], byte[]> directorsMap = values.getFamilyMap("directors".getBytes());
            //iterate directors family and construct JSON
            for (byte[] dir : directorsMap.descendingKeySet()) {
            json.append("{");
                json.append("\"id\":");
                json.append(new String(dir)).append(",");
                json.append("\"name\":");
                json.append("\"").append(new String(directorsMap.get(dir))).append("\"");
                json.append("},");               
            }
            json.deleteCharAt(json.lastIndexOf(","));
            json.append("]");
            
            json.append("}}");
            
            System.out.println(json);
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
