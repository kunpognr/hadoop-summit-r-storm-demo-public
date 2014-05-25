package com.mapr.hsummit.demo;

import com.google.common.collect.Lists;
import com.mapr.hsummit.demo.model.DataPoint;

import java.util.EnumMap;
import java.util.LinkedList;

/**
 * Created by arod on 25/05/2014.
 */
public class DataHolder {
    enum DataPointSeries {
        SCORE,
        LEVEL
    };

    static EnumMap<DataPointSeries, LinkedList<DataPoint>> dataSeries;

    {
        dataSeries = new EnumMap<DataPointSeries, LinkedList<DataPoint>>(DataPointSeries.class);
        dataSeries.put(DataPointSeries.SCORE, Lists.<DataPoint>newLinkedList());
        dataSeries.put(DataPointSeries.LEVEL, Lists.<DataPoint>newLinkedList());
    }
}
