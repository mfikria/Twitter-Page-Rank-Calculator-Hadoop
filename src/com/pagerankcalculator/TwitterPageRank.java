/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package com.pagerankcalculator;


import java.io.IOException;
import java.util.HashSet;
import java.util.Set;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.conf.*;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.util.*;
import com.pagerankcalculator.graphparsing.*;
import com.pagerankcalculator.calculation.*;
import com.pagerankcalculator.ordering.DoubleSortDescComparator;
import com.pagerankcalculator.ordering.PageRankSortingMapper;
import com.pagerankcalculator.ordering.PageRankSortingReducer;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.KeyValueTextInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.LazyOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

/**
 *
 * @author mfikria
 */
public class TwitterPageRank extends Configured implements Tool{
    public static String AUTHOR = "mfikria";
    
    public static Set<String> NODES = new HashSet<String>();
    
    public static int TOP_USER_COUNT = 5;
    public static int NUM_REDUCE_TASKS = 120;
    
    // Configuration
    public static Double DAMPING_FACTOR = 0.85;
    public static int ITERATIONS = 3;
    public static String IN_PATH = "";
    public static String OUT_PATH = "";
    public static String ACTION = "";
    public static String FOLLOWING_LIST_TAG = "|";
    public static String FOLLOWING_LIST_DELIMETER = ",";
    
    /**
     * Graph Parsing
     * Memasukan data mentah dan melakukan inisialisasi pagerank
     * 
     * @param in file data masukan
     * @param out direktori output
     */
    public int parseGraph(String in, String out) throws IOException, InterruptedException, ClassNotFoundException {
        
        Job job = Job.getInstance(getConf());
        job.setJobName("[" + TwitterPageRank.AUTHOR + "]: Job#1 Parsing Graph");
        job.setJarByClass(TwitterPageRank.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        job.setMapperClass(GraphParsingMapper.class);
        job.setReducerClass(GraphParsingReducer.class);

        job.setInputFormatClass(TextInputFormat.class);
        job.setNumReduceTasks(TwitterPageRank.NUM_REDUCE_TASKS);
        
        LazyOutputFormat.setOutputFormatClass(job, TextOutputFormat.class);
        
        Path inputFilePath = new Path(in);
        Path outputFilePath = new Path(out);
        
        FileInputFormat.addInputPath(job, inputFilePath);
        FileOutputFormat.setOutputPath(job, outputFilePath);
        
        
        FileSystem fs = FileSystem.newInstance(getConf());

        if (fs.exists(outputFilePath)) {
            fs.delete(outputFilePath, true);
        }
        
        return job.waitForCompletion(true) ? 0 : 1;
    }
    
    public int calculatePagerank(String in, String out, int iteration) throws IOException, InterruptedException, ClassNotFoundException {      
        Job job = Job.getInstance(getConf());    
        job.setJobName("[" + TwitterPageRank.AUTHOR + "]: Job#2 Iteration-" + iteration +" Calculating Page Rank");
        job.setJarByClass(TwitterPageRank.class);
        
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        job.setMapperClass(PageRankCalculationMapper.class);
        job.setReducerClass(PageRankCalculationReducer.class);

        job.setInputFormatClass(TextInputFormat.class);
        job.setNumReduceTasks(TwitterPageRank.NUM_REDUCE_TASKS);
        
        LazyOutputFormat.setOutputFormatClass(job, TextOutputFormat.class);
        
        Path inputFilePath = new Path(in);
        Path outputFilePath = new Path(out);
        
        FileInputFormat.addInputPath(job, inputFilePath);
        FileOutputFormat.setOutputPath(job, outputFilePath);
        
        FileSystem fs = FileSystem.newInstance(getConf());

        if (fs.exists(outputFilePath)) {
            fs.delete(outputFilePath, true);
        }

        return job.waitForCompletion(true) ? 0 : 1;
    }
    
    public int sortPagerank(String in, String out) throws IOException, InterruptedException, ClassNotFoundException {
        Job job = Job.getInstance(getConf());
        job.setJobName("[" + TwitterPageRank.AUTHOR + "]: Job#3 Sorting Page Rank");
        job.setJarByClass(TwitterPageRank.class);
        
        job.setMapOutputKeyClass(DoubleWritable.class);
        job.setMapOutputValueClass(Text.class);
                
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);
        
        job.setMapperClass(PageRankSortingMapper.class);
        job.setReducerClass(PageRankSortingReducer.class);
        
        job.setInputFormatClass(TextInputFormat.class);
        job.setNumReduceTasks(1);
        
        LazyOutputFormat.setOutputFormatClass(job, TextOutputFormat.class);
        
        job.setSortComparatorClass(DoubleSortDescComparator.class);
        
        Path inputFilePath = new Path(in);
        Path outputFilePath = new Path(out);
        
        FileInputFormat.addInputPath(job, inputFilePath);
        FileOutputFormat.setOutputPath(job, outputFilePath);
        
        FileSystem fs = FileSystem.newInstance(getConf());

        if (fs.exists(outputFilePath)) {
            fs.delete(outputFilePath, true);
        }

        return job.waitForCompletion(true) ? 0 : 1;
    }
        
    public static void main(String[] args) throws Exception {
        TwitterPageRank pageRankCalculator = new TwitterPageRank();
        int res = ToolRunner.run(pageRankCalculator, args);
        System.exit(res);
    }

    @Override
    public int run(String[] args) throws Exception {
        Integer iteration = null;
        int result = 0;
        
        if(args.length >= 3) {
            System.out.println(args[0]);
            System.out.println(args[1]);
            System.out.println(args[2]);
            ACTION = args[0].toLowerCase();
            IN_PATH = args[1];
            OUT_PATH = args[2];
        }
        
        if(args.length >= 4) {
            iteration = new Integer(args[3]);
        }
        
        switch(ACTION) {
            case "prepare":
                result = parseGraph(IN_PATH, OUT_PATH);
                break;
            case "execute":
                result = calculatePagerank(IN_PATH, OUT_PATH, iteration);
                break;
            case "sort":
                result = sortPagerank(IN_PATH, OUT_PATH);
                break;
        }
        return result;
    }
}
