/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package com.pagerankcalculator.graphparsing;

import java.io.IOException;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

/**
 *
 * @author mfikria
 */
public class GraphParsingMapper extends Mapper<LongWritable, Text, Text, Text>{
    
    private String userID;
    private String followerID;
    
    @Override
    public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        
        int tabIndex = value.find("\t");
 
        userID = Text.decode(value.getBytes(), 0, tabIndex);
        followerID = Text.decode(value.getBytes(), tabIndex + 1, value.getLength() - (tabIndex + 1));
        context.write(new Text(followerID),
                        new Text(userID));
    }
}
