package com.mapr.hsummit.demo;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.Lists;
import com.mapr.hsummit.demo.model.DataPoint;

import javax.servlet.ServletException;
import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import java.io.IOException;
import java.util.LinkedList;

public class ResultsServlet extends HttpServlet {
    static int DATA_POINT_LIMIT = 500;
    final LinkedList<DataPoint> dataPoints = Lists.newLinkedList();
    ObjectMapper mapper;

    {
        mapper = new ObjectMapper();
    }

    public ResultsServlet() {

    }

    protected void doGet(HttpServletRequest request, HttpServletResponse response) throws ServletException, IOException {
//        List<DataPoint> values = Lists.newArrayList();
//
//        int dur = 120;
//        DateTime now = DateTime.now().minusSeconds(dur*2);
//        for (int i = 0; i < dur; i++) {
//            DateTime ts = now.plusSeconds(2*i);
//            DataPoint dp = new DataPoint();
//            dp.setTimestamp(ts);
//            dp.setValue(Math.random() * 100);
//            values.add(dp);
//        }

        response.setContentType("application/json");
        response.setStatus(HttpServletResponse.SC_OK);
        response.getWriter().println(mapper.writeValueAsString(dataPoints));
    }
}