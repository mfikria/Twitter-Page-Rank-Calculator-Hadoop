/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package com.pagerankcalculator.graphparsing;

import com.pagerankcalculator.TwitterPageRank;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.output.MultipleOutputs;

/**
 *
 * @author mfikria
 */
public class GraphParsingReducer extends Reducer<Text, Text, Text, Text>{
    Integer initialPageRank = 1;

    @Override
    public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
        List<String> followingIDs = new ArrayList<>();
        
        for(Text value : values) {
            followingIDs.add(value.toString());
        }
        
        String CSVFollowingIDs = followingIDs.toString()
                                .replace("[", "")
                                .replace("]", "")
                                .replace(", ", TwitterPageRank.FOLLOWING_LIST_DELIMETER);
        
        String pageRankWithFollowingIDs = initialPageRank.toString() + "\t" + CSVFollowingIDs;
        
        context.write(key, new Text(pageRankWithFollowingIDs));
    }
}
