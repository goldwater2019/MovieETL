package com.xxx.movie.etl;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;

public class EtlDriver {
    public static void main(String[] args2) throws IOException, InterruptedException, ClassNotFoundException {
        String[] args = {"hello.txt", "out"};
        Job job = Job.getInstance(new Configuration());

        job.setJarByClass(EtlDriver.class);
        job.setMapperClass(EtlMapper.class);

        // 不需要 reduce
        job.setNumReduceTasks(0);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(NullWritable.class);

        FileInputFormat.setInputPaths(job, new Path(args[0]));

        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        boolean b = job.waitForCompletion(true);
        System.exit(b ? 0 : 1);
    }
}
