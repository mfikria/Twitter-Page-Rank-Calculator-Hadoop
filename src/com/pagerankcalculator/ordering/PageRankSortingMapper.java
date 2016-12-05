/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package com.pagerankcalculator.ordering;

import java.io.IOException;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

/**
 *
 * @author mfikria
 */
public class PageRankSortingMapper extends Mapper<LongWritable, Text, DoubleWritable, Text> {

    @Override
    public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        int tabIdx1 = value.find("\t");
        int tabIdx2 = value.find("\t", tabIdx1 + 1);
        
        String username = Text.decode(value.getBytes(), 0, tabIdx1);
        
        Double pageRank = new Double(Text.decode(value.getBytes(), tabIdx1 + 1, tabIdx2 - (tabIdx1 + 1)));
        
        context.write(new DoubleWritable(pageRank),
                       new Text(username));
    }
    
}
