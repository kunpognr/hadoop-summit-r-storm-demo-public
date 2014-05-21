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

import java.util.Map;

/**
 * Created by syoon on 5/20/14.
 */
public class JettyServerBolt extends BaseRichBolt {
    ResultsServlet rs;
    OutputCollector collector;

    public JettyServerBolt() {

        System.out.println("Initializing server...");
        final ServletContextHandler context =
                new ServletContextHandler(ServletContextHandler.SESSIONS);
        context.setContextPath("/");
        context.setResourceBase("src/main/webapp");

        context.setClassLoader(
                Thread.currentThread().getContextClassLoader()
        );

        context.addServlet(DefaultServlet.class, "/");
        rs = new ResultsServlet();
        context.addServlet(new ServletHolder(rs), "/data/");

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
            JSONArray ja = (JSONArray) tuple.getValues();
            JSONArray ja_in = (JSONArray) ja.get(0);
            DataPoint dp = new DataPoint();
            dp.setTimestamp(new DateTime(Long.parseLong((String)ja_in.get(0)) * 1000000));
            dp.setValue(Double.valueOf((String)ja_in.get(1)));
            rs.dataPoints.add(dp);
            if (rs.dataPoints.size() >= ResultsServlet.DATA_POINT_LIMIT) {
                rs.dataPoints.removeFirst();
            }
            System.out.println("data point size " + rs.dataPoints.size());
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {
        outputFieldsDeclarer.declare(new Fields("webappoutput"));
    }
}
