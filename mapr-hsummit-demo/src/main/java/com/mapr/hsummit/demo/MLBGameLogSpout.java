package com.mapr.hsummit.demo;

import backtype.storm.spout.SpoutOutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichSpout;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Values;
import backtype.storm.utils.Utils;

import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
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
    static SimpleDateFormat sdf  = new SimpleDateFormat("yyyyMMdd");
    int win = 0, loss = 0, tie = 0;

    @Override
    public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {
        outputFieldsDeclarer.declare(new Fields("ts", "sep", "winloss"));
    }

    @Override
    public void open(Map map, TopologyContext topologyContext, SpoutOutputCollector spoutOutputCollector) {
        _collector = spoutOutputCollector;
        try {
            br = new BufferedReader(new FileReader(HSummitTopology.GAMELOGFILE));
        } catch (FileNotFoundException e) {
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
                    if ( elems[3].replace("\"","").equals("SFN") ) {
                        giantsGame = true;
                        sfnscore = Integer.parseInt(elems[9]);
                        otherscore = Integer.parseInt(elems[10]);
                    }
                    if ( elems[6].replace("\"", "").equals("SFN")) {
                        giantsGame = true;
                        sfnscore = Integer.parseInt(elems[10]);
                        otherscore = Integer.parseInt(elems[9]);
                    }
                    if ( giantsGame ) {

                        Date date = null;
                        try {
                            date = sdf.parse(elems[0].replace("\"", ""));
                            long timeInMillisSinceEpoch = date.getTime();
                            if (sfnscore > otherscore) {
                                win++;
                            } else if (sfnscore < otherscore) {
                                loss++;
                            } else {
                                tie++;
                            }
                            _collector.emit(new Values(timeInMillisSinceEpoch/1000000, ":", (float)win/(float)(win+loss+tie)));
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
