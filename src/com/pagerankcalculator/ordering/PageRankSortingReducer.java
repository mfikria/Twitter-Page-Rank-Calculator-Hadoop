/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package com.pagerankcalculator.ordering;

import com.pagerankcalculator.TwitterPageRank;
import java.io.IOException;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

/**
 *
 * @author mfikria
 */
public class PageRankSortingReducer extends Reducer<DoubleWritable, Text, Text, Text> {
    int counter = 0;
    
    @Override
    protected void setup(Context context) {
        counter = 0;
    }
    
    @Override
    public void reduce(DoubleWritable key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
        
        for(Text value : values) {
            if(counter < TwitterPageRank.TOP_USER_COUNT) {
                counter++;
                context.write(value, new Text(key.toString()));
            } else {
                break;
            }
        }
    }
}
