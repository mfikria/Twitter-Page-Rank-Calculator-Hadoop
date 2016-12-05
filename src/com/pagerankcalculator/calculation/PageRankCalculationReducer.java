/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package com.pagerankcalculator.calculation;

import com.pagerankcalculator.TwitterPageRank;
import java.io.IOException;
import java.util.Iterator;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.output.MultipleOutputs;

/**
 *
 * @author mfikria
 */
public class PageRankCalculationReducer extends Reducer<Text, Text, Text, Text>{ 
//    private MultipleOutputs<Text, Text> multipleOutputs;
    
    @Override
    public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
        String CSVFollowingIDs = "";
        Double sumOtherPageRanks = 0.0;
        
        for (Text value : values) {
            String content = value.toString();
            
            if (content.startsWith(TwitterPageRank.FOLLOWING_LIST_TAG)) {
               CSVFollowingIDs += content.substring(TwitterPageRank.FOLLOWING_LIST_TAG.length());
            } else {
                String[] pageRankWithTotalFollowing = content.split("\t");
                
                Double pageRank = new Double(pageRankWithTotalFollowing[0]);
                Integer totalFollowing = new Integer(pageRankWithTotalFollowing[1]);
                
                sumOtherPageRanks += (pageRank / totalFollowing);
            }
        }
        
        Double newPageRank = (1 - TwitterPageRank.DAMPING_FACTOR) + (TwitterPageRank.DAMPING_FACTOR * sumOtherPageRanks);
        Text pageRankWithFollowingIDs = new Text(newPageRank.toString() + "\t" + CSVFollowingIDs);
        context.write(key, pageRankWithFollowingIDs);
    }
    
//    @Override
//    public void setup(Context context){
//        multipleOutputs = new MultipleOutputs<Text, Text>(context);
//    }
//	
//    @Override
//    public void cleanup(final Context context) throws IOException, InterruptedException{
//        multipleOutputs.close();
//    }
//    
}
