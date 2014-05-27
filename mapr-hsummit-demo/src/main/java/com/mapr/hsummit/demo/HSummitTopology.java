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

    //static String RSCRIPT = HSummitTopology.class.getResource("/storm.R").getFile();

    public static void main(String[] args) throws Exception {
        if ( args.length != 4) {
            System.out.println("usage path datafilename");
        }
        String resourcePath = args[0];
        String dataFilename = args[1];
        String rFilename = args[2];
        String teamName = args[3];

        DataHolder dataHolder = new DataHolder();
        TopologyBuilder builder = new TopologyBuilder();
        builder.setSpout("spout", new MLBGameLogSpout(resourcePath,dataFilename, teamName), 1);
        builder.setBolt("count", new RBolt(rFilename), 1).shuffleGrouping("spout");
        //builder.setBolt("print", new PrinterBolt(), 1).shuffleGrouping("count");
        builder.setBolt("serve", new JettyServerBolt(resourcePath), 1).shuffleGrouping("count");

        Config conf = new Config();
        conf.setDebug(true);

        conf.setMaxTaskParallelism(1);
        LocalCluster cluster = new LocalCluster();
        cluster.submitTopology("hadoop summit test", conf, builder.createTopology());
        Thread.sleep(3000000);
        cluster.shutdown();
//        if (args != null && args.length > 0) {
//            conf.setNumWorkers(3);
//            StormSubmitter.submitTopology(args[0], conf, builder.createTopology());
//        }
//        else {
//
//        }
    }
}
