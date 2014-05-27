package com.mapr.hsummit.demo;

import backtype.storm.spout.SpoutOutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichSpout;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Values;
import backtype.storm.utils.Utils;

import java.io.*;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.Map;

/**
 * Created by syoon on 5/19/14.
 */
public class MLBGameLogSpout extends BaseRichSpout {

    SpoutOutputCollector _collector;
    BufferedReader br = null;
    String theFile="";
    String teamName="";
    static SimpleDateFormat sdf  = new SimpleDateFormat("yyyyMMdd");
    int win = 0, loss = 0, tie = 0;

    public MLBGameLogSpout(String resourcePath, String dataFileName, String teamName) {
        theFile = resourcePath + "/" + dataFileName;
        this.teamName = teamName;
    }
    @Override
    public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {
        outputFieldsDeclarer.declare(new Fields("ts", "sep", "winloss"));
    }

    @Override
    public void open(Map map, TopologyContext topologyContext, SpoutOutputCollector spoutOutputCollector) {
        _collector = spoutOutputCollector;
        try {
            //br = new BufferedReader(new InputStreamReader(HSummitTopology.class.getResourceAsStream("/GL2012.TXT")));
            br = new BufferedReader(new FileReader(theFile));
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    @Override
    public void close() {
        if ( br != null ) {
            try {
                br.close();
                br = null;
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
    }


    @Override
    public void ack(Object id) {
    }

    @Override
    public void fail(Object id) {
    }

    @Override
    public void nextTuple() {
        Utils.sleep(50);
        if ( br != null ) {
            try {
                String line = br.readLine();
                if ( line == null ) {
                    this.close();
                    br = null;
                    return;
                }
                String [] elems = line.split(",");
                int sfnscore = 0;
                int otherscore = 0;
                boolean giantsGame = false;
                if ( elems.length > 15 ) {
                    if ( elems[3].replace("\"","").equals(teamName) ) {
                        giantsGame = true;
                        sfnscore = Integer.parseInt(elems[9]);
                        otherscore = Integer.parseInt(elems[10]);
                    }
                    if ( elems[6].replace("\"", "").equals(teamName)) {
                        giantsGame = true;
                        sfnscore = Integer.parseInt(elems[10]);
                        otherscore = Integer.parseInt(elems[9]);
                    }
                    if ( giantsGame ) {

                        Date date = null;
                        try {
                            String dateString = elems[0];
                            if ( elems[0].contains(":")) {
                                String[] elems0split = elems[0].split(":");
                                if ( elems0split.length == 2) {
                                    dateString = elems0split[1];
                                } else {
                                    System.out.println("can't parse " + elems[0]);
                                    return;
                                }
                            }
                            System.out.println("date string " + dateString);
                            date = sdf.parse(dateString.replace("\"", ""));
                            long timeInMillisSinceEpoch = date.getTime();
                            if (sfnscore > otherscore) {
                                win++;
                            } else if (sfnscore < otherscore) {
                                loss++;
                            } else {
                                tie++;
                            }
                            double dataToSend = sfnscore - otherscore;
                            if ( dataToSend > 10 ) {
                                dataToSend = 10;
                            }
                            if ( dataToSend < -10) {
                                dataToSend = -10;
                            }
                            _collector.emit(new Values(timeInMillisSinceEpoch/1000000, ":", dataToSend));//(float)win/(float)(win+loss+tie)));
                        } catch (ParseException e) {
                            e.printStackTrace();
                        }

                    }
                }
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
    }
}
