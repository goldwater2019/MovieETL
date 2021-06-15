package com.xxx.movie.etl;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Counter;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class EtlMapper extends Mapper<LongWritable, Text, Text, NullWritable> {

    private Counter pass;
    private Counter failed;
    private StringBuilder sb;
    private Text result = new Text();

    @Override
    protected void setup(Context context) throws IOException, InterruptedException {
        pass = context.getCounter("ETL", "pass");
        failed = context.getCounter("ETL", "failed");
    }

    /**
     * LKh7zAJ4nwo     TheReceptionist 653     People & Blogs   424     13021   4.34    1305    744     DjdA-5oKYFQ     NxTDlnOuybo     c-8VuICzXtU     DH56yrIO5nI     W1Uo5DQTtzc     E-3zXq_r4w0     1TCeoRPg5dE     yAr26YhuYNY     2ZgXx72XmoE       -7ClGo-YgZ0     vmdPOOd6cxI     KRHfMQqSHpk     pIMpORZthYw     1tUDzOp10pk     heqocRij5P0     _XIuvoH6rUg     LGVU5DsezE0     uO2kj6_D8B4     xiDqywcDQRM     uX81lMev6_o
     *
     *
     * 将一行日志数据进行处理
     * 字段不够的抛弃
     * 第四个字段的" & " -> "&"
     * 最后相关的字段的"\t" -> "&"
     * @param key
     * @param value
     * @param context
     * @throws IOException
     * @throws InterruptedException
     */
    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        // 一行数据
        String line = value.toString();
        // 将数据切分
        String[] fields = line.split("\t");
        // 判断数量
        if (fields.length >= 9) {

            // 拼接之前, 需要把字符串清零
            sb.setLength(0);
            // 去掉第四个字段的空格
            fields[3] = fields[3].replace(" & ", "&");
            // 去掉最后那些字段的\t
            for (int i = 0; i < fields.length; i++) {
                if (i == fields.length - 1) {
                    sb.append(fields[i]);
                } else if (i < 9) {
                    sb.append(fields[i]).append("\t");
                } else {
                    sb.append(fields[i]).append("&");
                }
            }
            result.set(sb.toString());
            context.write(result, NullWritable.get());
            pass.increment(1);
        } else {
            // 数据不要了
            failed.increment(1);
        }
    }
}
