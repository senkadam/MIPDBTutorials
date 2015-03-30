package cz.cvut.fit.senkadam.mipdb.hbase.plainmapreduce;


import org.apache.commons.io.FileUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

import java.io.File;
import java.io.IOException;
import java.util.Iterator;


/**
 * Created by senk on 11.3.15.
 */

/**
 * Mapper parsing input file and creating sequence file
 */
public class WordLength {



    public static class Map extends Mapper<LongWritable, Text,IntWritable, Text> {


        @Override
        public void map(LongWritable key, Text value, Mapper.Context context) throws IOException, InterruptedException {
            String[] lines = value.toString().split("\\n");

            for (String line : lines) {

                String[] words = line.toString().split("\\s+");

                for (String word : words) {

                    context.write(new IntWritable(word.length()), new Text(word));
                }
            }


        }

    }

    public static class Reduce extends Reducer<IntWritable, Text ,IntWritable,Text> {

        private IntWritable count = new IntWritable();


        @Override
        public void reduce(IntWritable key,  Iterable<Text> values, Context context) throws IOException, InterruptedException {


            String result="";
            int sum=0;
            for (Text val : values) {
                result += " ; " +val.toString();
                sum++;

            }

            count.set(sum);

            context.write(key,new Text(result+" - - - "+count));


        }

    }




    public static void main(String[] args) throws Exception {


        FileUtils.deleteDirectory(new File("data/output"));

        Configuration config = new Configuration();
        Job job= Job.getInstance(config, "Word Count");

        //job.setNumReduceTasks(1);
        //job.setMapOutputKeyClass(BytesWritable.class);
        //job.setMapOutputValueClass(IntWritable.class);

        job.setOutputKeyClass(IntWritable.class);
        job.setOutputValueClass(Text.class);

        job.setMapperClass(Map.class);
        //job.setReducerClass(Reducer.class);
        job.setCombinerClass(Reduce.class);
        job.setReducerClass(Reduce.class);

        job.setOutputFormatClass(TextOutputFormat.class);
        job.setInputFormatClass(TextInputFormat.class);

        FileInputFormat.setInputPaths(job, new Path("data/"));
        FileOutputFormat.setOutputPath(job, new Path("data/output"));

        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }

}

