/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package com.pagerankcalculator.calculation;

import com.pagerankcalculator.TwitterPageRank;
import java.io.IOException;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

/**
 *
 * @author mfikria
 */
public class PageRankCalculationMapper extends Mapper<LongWritable, Text, Text, Text> {

    @Override
    public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        
        int tabIdx1 = value.find("\t");
        int tabIdx2 = value.find("\t", tabIdx1 + 1);
        
        String userID = Text.decode(value.getBytes(), 0, tabIdx1);
        String pageRank = Text.decode(value.getBytes(), tabIdx1 + 1, tabIdx2 - (tabIdx1 + 1));
        String CSVFollowingIDs = Text.decode(value.getBytes(), tabIdx2 + 1, value.getLength() - (tabIdx2 + 1));
        
//        System.out.print(userID);
//        System.out.print("\t");
//        System.out.print(pageRank);
//        System.out.print("\t");
//        System.out.println(CSVFollowingIDs);
        
        String[] followingIDs = CSVFollowingIDs.split(TwitterPageRank.FOLLOWING_LIST_DELIMETER);
        Integer totalFollowingIDs = followingIDs.length;
        for (String followingID : followingIDs) {
            String pageRankWithTotalFollowing = pageRank + "\t" + totalFollowingIDs.toString();
            
            context.write(new Text(followingID),
                           new Text(pageRankWithTotalFollowing));
        }
        
        context.write(new Text(userID),
                       new Text(TwitterPageRank.FOLLOWING_LIST_TAG + CSVFollowingIDs));
    }
    
}
