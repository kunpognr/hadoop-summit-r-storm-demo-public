package com.mapr.hsummit.demo;

import backtype.storm.Config;
import backtype.storm.LocalCluster;
import backtype.storm.StormSubmitter;
import backtype.storm.topology.TopologyBuilder;

/**
 * Created by syoon on 5/15/14.
 */
public class HSummitTopology {
//    static String workingDir = System.getProperty("user.dir");
//    static String GAMELOGFILE = workingDir + "/src/main/resources/GL2012.TXT";
//    static String RSCRIPT = workingDir + "/src/main/resources/storm.R";

    static String GAMELOGFILE = HSummitTopology.class.getResource("/GL2012.TXT").getFile();
    static String RSCRIPT = HSummitTopology.class.getResource("/storm.R").getFile();

    public static void main(String[] args) throws Exception {
        DataHolder dataHolder = new DataHolder();
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

            Thread.sleep(300000);

            cluster.shutdown();
        }
    }
}
