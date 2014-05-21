package com.mapr.hsummit.demo;

import backtype.storm.Config;
import backtype.storm.task.ShellBolt;
import backtype.storm.topology.IRichBolt;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.tuple.Fields;

import java.util.Map;

/**
 * Created by syoon on 5/19/14.
 */
public class RBolt  extends ShellBolt implements IRichBolt {

    public RBolt() {
        super("Rscript", HSummitTopology.RSCRIPT);
    }
    @Override
    public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {
        outputFieldsDeclarer.declare(new Fields("tswinratio"));
    }

    @Override
    public Map<String, Object> getComponentConfiguration() {
        Config ret = new Config();
        ret.setMaxTaskParallelism(1);
        return ret;
    }
}
