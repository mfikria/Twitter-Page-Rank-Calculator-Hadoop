/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package com.pagerankcalculator.ordering;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;

/**
 *
 * @author mfikria
 */
public class DoubleSortDescComparator extends WritableComparator {
    protected DoubleSortDescComparator() {
        super(DoubleWritable.class, true);
    }
    
    @SuppressWarnings("rawtypes")
    @Override
    public int compare(WritableComparable w1, WritableComparable w2) {
        DoubleWritable k1 = (DoubleWritable) w1;
        DoubleWritable k2 = (DoubleWritable) w2;
        
        return -1 * k1.compareTo(k2);
    }
}
