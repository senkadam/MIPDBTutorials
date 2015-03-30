package cz.cvut.fit.senkadam.mipdb.plainmapreduce;


import org.apache.commons.io.FileUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
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


/**
 * Created by senk on 11.3.15.
 */

/**
 * Mapper parsing input file and creating sequence file
 */
public class WordCount {



    public static class Map extends Mapper<LongWritable, Text,Text, IntWritable> {


        @Override
        public void map(LongWritable key, Text value, Mapper.Context context) throws IOException, InterruptedException {
            String[] lines = value.toString().split("\\n");

            for (String line : lines) {

                String[] words = line.toString().split("\\s+");

                for (String word : words) {

                    context.write(new Text(word), new IntWritable(1));
                }
            }


        }

    }

    public static class Reduce extends Reducer<Text, IntWritable ,Text, IntWritable> {


        @Override
        public void reduce(Text key,  Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {

            int sum = 0;
            for (IntWritable val : values) {
                sum += val.get();
            }


            context.write(key, new IntWritable(sum));


        }

    }




    public static void main(String[] args) throws Exception {


        FileUtils.deleteDirectory(new File("data/output"));

        Configuration config = new Configuration();
        Job job= Job.getInstance(config, "Word Count");

        //job.setNumReduceTasks(1);
        //job.setMapOutputKeyClass(BytesWritable.class);
        //job.setMapOutputValueClass(IntWritable.class);



        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);

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

