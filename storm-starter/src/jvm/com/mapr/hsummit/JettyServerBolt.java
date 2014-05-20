package com.mapr.hsummit;

import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichBolt;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import org.eclipse.jetty.server.Request;
import org.eclipse.jetty.server.Server;
import org.eclipse.jetty.server.handler.AbstractHandler;

import javax.servlet.ServletException;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import java.io.IOException;
import java.util.Map;

/**
 * Created by syoon on 5/20/14.
 */
public class JettyServerBolt extends BaseRichBolt {

    public static class HelloWorld extends AbstractHandler {

        @Override
        public void handle(String s, Request baseRequest, HttpServletRequest httpServletRequest,
                           HttpServletResponse response) throws IOException, ServletException {
            response.setContentType("text/html;charset=utf-8");
            response.setStatus(HttpServletResponse.SC_OK);
            baseRequest.setHandled(true);
            response.getWriter().println("<h1>Hello World</h1>");
        }
    }
    OutputCollector collector;

    public JettyServerBolt() {
        Server server = new Server(8080);
        server.setHandler(new HelloWorld());

        try {
            server.start();
            //server.join();
        } catch (Exception e) {
            e.printStackTrace();
        }

    }

    @Override
    public void prepare(Map map, TopologyContext topologyContext, OutputCollector outputCollector) {
        this.collector = outputCollector;
    }

    @Override
    public void execute(Tuple tuple) {
        System.out.println(tuple.getString(0));
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {
        outputFieldsDeclarer.declare(new Fields("webappoutput"));
    }
}
