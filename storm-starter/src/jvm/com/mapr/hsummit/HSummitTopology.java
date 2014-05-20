package com.mapr.hsummit;

import backtype.storm.Config;
import backtype.storm.LocalCluster;
import backtype.storm.StormSubmitter;
import backtype.storm.topology.BasicOutputCollector;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.TopologyBuilder;
import backtype.storm.topology.base.BaseBasicBolt;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;
import storm.starter.bolt.PrinterBolt;

import java.util.HashMap;
import java.util.Map;

/**
 * Created by syoon on 5/15/14.
 */
public class HSummitTopology {


    public static void main(String[] args) throws Exception {

        TopologyBuilder builder = new TopologyBuilder();
        builder.setSpout("spout", new MLBGameLogSpout(), 1);
        builder.setBolt("count", new RBolt(), 1).shuffleGrouping("spout");
        //builder.setBolt("print", new PrinterBolt(), 1).shuffleGrouping("count");
        builder.setBolt("serve", new JettyServerBolt(), 1).shuffleGrouping("count");

        Config conf = new Config();
        conf.setDebug(true);


        if (args != null && args.length > 0) {
            conf.setNumWorkers(3);
            StormSubmitter.submitTopology(args[0], conf, builder.createTopology());
        }
        else {
            conf.setMaxTaskParallelism(3);

            LocalCluster cluster = new LocalCluster();
            cluster.submitTopology("hadoop summit test", conf, builder.createTopology());

            Thread.sleep(30000);

            cluster.shutdown();
        }
    }
}
