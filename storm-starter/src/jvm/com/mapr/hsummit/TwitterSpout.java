package com.mapr.hsummit;

import backtype.storm.Config;
import backtype.storm.spout.SpoutOutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichSpout;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Values;
import backtype.storm.utils.Utils;
import twitter4j.*;
import twitter4j.auth.AccessToken;
import twitter4j.conf.ConfigurationBuilder;

import java.util.Map;
import java.util.concurrent.LinkedBlockingQueue;

/**
 * Created by syoon on 5/15/14.
 */
public class TwitterSpout extends BaseRichSpout {

    SpoutOutputCollector _collector;
    LinkedBlockingQueue<Status> queue = null;
    TwitterStream _twitterStream;
    String consumerKey;
    String consumerSecret;
    String accessToken;
    String accessTokenSecret;
    String[] keyWords;

    public TwitterSpout(String consumerKey, String consumerSecret,
                              String accessToken, String accessTokenSecret, String[] keyWords) {
        this.consumerKey = consumerKey;
        this.consumerSecret = consumerSecret;
        this.accessToken = accessToken;
        this.accessTokenSecret = accessTokenSecret;
        this.keyWords = keyWords;
    }

    public TwitterSpout() {
        this.consumerKey = "CioFFk7a09dFOtLNzpgSmF27H";
        this.consumerSecret = "JrozX9lcxmzi62yJj0IIveFjsUn6en0R0kQkqd4g7XOvkVuKTP";
        this.accessToken = "295262058-F2DIWIqh48Luc3xi2fT3tPlW6KxqlkCfDuYKUJKq";
        this.accessTokenSecret = "EFJOAmFqBhRVFCqkJ2G7uOBaVjbaNZMSRDeya7G9hqGJ4";
        this.keyWords = new String[] {};
    }

    @Override
    public void open(Map conf, TopologyContext context,
                     SpoutOutputCollector collector) {
        queue = new LinkedBlockingQueue<Status>(1000);
        _collector = collector;

        StatusListener listener = new StatusListener() {

            @Override
            public void onStatus(Status status) {

                queue.offer(status);
            }

            @Override
            public void onDeletionNotice(StatusDeletionNotice sdn) {
            }

            @Override
            public void onTrackLimitationNotice(int i) {
            }

            @Override
            public void onScrubGeo(long l, long l1) {
            }

            @Override
            public void onException(Exception ex) {
            }

            @Override
            public void onStallWarning(StallWarning arg0) {
                // TODO Auto-generated method stub

            }

        };

        TwitterStream twitterStream = new TwitterStreamFactory(
                new ConfigurationBuilder().setJSONStoreEnabled(true).build())
                .getInstance();

        twitterStream.addListener(listener);
        twitterStream.setOAuthConsumer(consumerKey, consumerSecret);
        AccessToken token = new AccessToken(accessToken, accessTokenSecret);
        twitterStream.setOAuthAccessToken(token);

        if (keyWords.length == 0) {

            twitterStream.sample();
        }

        else {

            FilterQuery query = new FilterQuery().track(keyWords);
            twitterStream.filter(query);
        }

    }

    @Override
    public void nextTuple() {
        Status ret = queue.poll();
        if (ret == null) {
            Utils.sleep(50);
        } else {
            try {
                GeoLocation gl = ret.getGeoLocation();
                double lat = gl.getLatitude();
                double lon = gl.getLongitude();
                if (lat > 18.005611 && lat < 48.987386 && lon > -124.626080 && lon < -62.361014) {
                    _collector.emit(new Values(ret.getText()));
                }
            } catch (Exception e ) {

            }
        }
    }

    @Override
    public void close() {
        _twitterStream.shutdown();
    }

    @Override
    public Map<String, Object> getComponentConfiguration() {
        Config ret = new Config();
        ret.setMaxTaskParallelism(1);
        return ret;
    }

    @Override
    public void ack(Object id) {
    }

    @Override
    public void fail(Object id) {
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declare(new Fields("tweet"));
    }

}
