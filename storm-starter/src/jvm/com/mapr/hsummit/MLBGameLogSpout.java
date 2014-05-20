package com.mapr.hsummit;

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
import java.util.Map;

/**
 * Created by syoon on 5/19/14.
 */
public class MLBGameLogSpout extends BaseRichSpout {

    SpoutOutputCollector _collector;
    BufferedReader br = null;

    @Override
    public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {
        outputFieldsDeclarer.declare(new Fields("gamelog"));
    }

    @Override
    public void open(Map map, TopologyContext topologyContext, SpoutOutputCollector spoutOutputCollector) {
        _collector = spoutOutputCollector;
        try {
            br = new BufferedReader(new FileReader("/Users/syoon/work/projects/hadoop-summit/data/GL2012.TXT"));
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
        Utils.sleep(100);
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
                    if ( elems[3].equals("\"SFN\"") ) {
                        giantsGame = true;
                        sfnscore = Integer.parseInt(elems[9]);
                        otherscore = Integer.parseInt(elems[10]);
                    }
                    if ( elems[6].equals("\"SFN\"")) {
                        giantsGame = true;
                        sfnscore = Integer.parseInt(elems[10]);
                        otherscore = Integer.parseInt(elems[9]);
                    }
                    if ( giantsGame ) {
                        if (sfnscore > otherscore) {
                            _collector.emit(new Values("win"));
                        } else if (sfnscore < otherscore) {
                            _collector.emit(new Values("loss"));
                        } else {
                            _collector.emit(new Values("tie"));
                        }
                    }
                }
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
    }
}
