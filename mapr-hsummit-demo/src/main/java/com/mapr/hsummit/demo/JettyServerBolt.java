package com.mapr.hsummit.demo;

import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichBolt;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import com.mapr.hsummit.demo.model.DataPoint;
import org.eclipse.jetty.server.Server;
import org.eclipse.jetty.servlet.DefaultServlet;
import org.eclipse.jetty.servlet.ServletContextHandler;
import org.eclipse.jetty.servlet.ServletHolder;
import org.joda.time.DateTime;
import org.json.simple.JSONArray;
import org.json.simple.JSONObject;

import java.util.LinkedList;
import java.util.Map;

/**
 * Created by syoon on 5/20/14.
 */
public class JettyServerBolt extends BaseRichBolt {
    OutputCollector collector;

    public JettyServerBolt(String resourcePath) {

        System.out.println("Initializing server...");
        final ServletContextHandler context =
                new ServletContextHandler(ServletContextHandler.SESSIONS);
        context.setContextPath("/");
        //context.setResourceBase(HSummitTopology.class.getResource("/webapp/").getFile());
        context.setResourceBase(resourcePath + "/webapp");
        context.setClassLoader(
                Thread.currentThread().getContextClassLoader()
        );

        context.addServlet(DefaultServlet.class, "/");
        context.addServlet(new ServletHolder(new ResultsServlet()), "/data/*");

        final Server server = new Server(8080);
        server.setHandler(context);

        System.out.println("Starting server...");
        try {
            server.start();
        } catch (Exception e) {
            System.out.println("Failed to start server!");
            return;
        }

        System.out.println("Server running...");
//        while (true) {
//            try {
//                server.join();
//            } catch (InterruptedException e) {
//                System.out.println("Server interrupted!");
//            }
//        }

    }

    @Override
    public void prepare(Map map, TopologyContext topologyContext, OutputCollector outputCollector) {
        this.collector = outputCollector;
    }

    @Override
    public void execute(Tuple tuple) {
        try {
            System.out.println("tuple size " + tuple.size());
            DateTime dt = new DateTime(tuple.getLong(1) * 1000000);
            double theValue = 0;
            try {
                theValue = tuple.getDouble(2);
            } catch (java.lang.ClassCastException e) {
                try {
                    theValue = (double) tuple.getLong(2);
                } catch (java.lang.ClassCastException e2) {

                }
            }
            System.out.println(tuple + "  --- " + theValue);
            if ( tuple.getLong(0) == 0 ) {
                DataPoint dp = new DataPoint();
                dp.setTimestamp(dt);
                dp.setValue(theValue);
                dp.setLevel(0.0);
                addDataPoint(dp, DataHolder.DataPointSeries.SCORE, false);

            } else if (tuple.getLong(0) == 1) {
                DataPoint levelDp = new DataPoint();
                levelDp.setTimestamp(dt);
                levelDp.setValue(theValue);
                addDataPoint(levelDp, DataHolder.DataPointSeries.LEVEL, true);
            } else if (tuple.getLong(0) == 2) {
                DataPoint levelDp = new DataPoint();
                levelDp.setTimestamp(dt);
                levelDp.setValue(theValue);
                addDataPoint(levelDp, DataHolder.DataPointSeries.LEVEL, false);
            }
            arrangeDataPoint();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    private void addDataPoint(DataPoint dp, DataHolder.DataPointSeries serie,
                              boolean reset) {
        final LinkedList<DataPoint> points = DataHolder.dataSeries.get(serie);
        if (reset) {
            points.clear();
        }
        points.add(dp);
        if (points.size() >= ResultsServlet.DATA_POINT_LIMIT) {
            points.removeFirst();
        }
        System.out.println("data point size " + points.size());
    }

    private void arrangeDataPoint() {
        // filling up level values onto score datapoints
        LinkedList<DataPoint> levelPoints = DataHolder.dataSeries.get(DataHolder.DataPointSeries.LEVEL);
        LinkedList<DataPoint> scorePoints = DataHolder.dataSeries.get(DataHolder.DataPointSeries.SCORE);
        if ( levelPoints.size() > 0 ) {
            int lpp = 0;
            for(int i=0; i < scorePoints.size(); i++) {
                if ( scorePoints.get(i).getTimestamp().compareTo(levelPoints.get(lpp).getTimestamp()) <= 0) {
                    scorePoints.get(i).setLevel(levelPoints.get(lpp).getValue());
                } else {
                    if ( lpp < levelPoints.size()-1 ) {
                        lpp++;
                    }
                    scorePoints.get(i).setLevel(levelPoints.get(lpp).getValue());
                }
            }
        }
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {
        outputFieldsDeclarer.declare(new Fields("webappoutput"));
    }
}
