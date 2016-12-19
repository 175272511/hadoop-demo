package com.example;

import com.example.hadoop.WordCount;
import com.google.common.collect.Lists;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mrunit.mapreduce.MapDriver;
import org.apache.hadoop.mrunit.mapreduce.ReduceDriver;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.util.ArrayList;

/**
 * Created by liuhui on 2016/12/16.
 */
public class WordCountAppTest {

    //单词统计Mapper
    private WordCount.WordCountMapper wordCountMapper;

    //单词统计Reducer
    private WordCount.WordCountReducer wordCountReducer;

    //Mapper和Reducer的Driver
    private MapDriver<LongWritable, Text, Text, LongWritable> mapDriver;
    private ReduceDriver<Text, LongWritable, Text, LongWritable> reduceDriver;
    //private MapReduceDriver mrDriver;

    @Before
    public void before(){
        this.wordCountMapper = new WordCount.WordCountMapper();
        this.wordCountReducer = new WordCount.WordCountReducer();

        this.mapDriver = MapDriver.newMapDriver(wordCountMapper);
        this.reduceDriver = ReduceDriver.newReduceDriver(wordCountReducer);
        //也可以这样写：同时测试map和reduce
        //this.mrDriver = MapReduceDriver.newMapReduceDriver(wordCountMapper, wordCountReducer);
    }

    @Test
    public void testMap() throws IOException {
        //设置输入数据
        this.mapDriver.addInput(new LongWritable(0), new Text("blog\txiaoxiaomo"));
        this.mapDriver.addInput(new LongWritable(0), new Text("xxo\tblog"));
        this.mapDriver.addOutput(new Text("blog"), new LongWritable(1));
        this.mapDriver.addOutput(new Text("xiaoxiaomo"), new LongWritable(1));
        this.mapDriver.addOutput(new Text("xxo"), new LongWritable(1));
        this.mapDriver.addOutput(new Text("blog"), new LongWritable(1));

        this.mapDriver.runTest();
    }

    @Test
    public void testReduce() throws IOException{
        ArrayList<LongWritable> values = Lists.newArrayList(new LongWritable(1), new LongWritable(2));
        this.reduceDriver.addInput(new Text("xiaoxiaomo"), values);
        this.reduceDriver.addInput(new Text("blog"), values);

        this.reduceDriver.run();
    }
}
