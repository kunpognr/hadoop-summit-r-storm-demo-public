package com.mapr.ps.web;

import com.fasterxml.jackson.databind.ObjectMapper;

import javax.servlet.ServletException;
import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import java.io.IOException;

public class ResultsServlet extends HttpServlet {
    final ResultFetcher fetcher;
    ObjectMapper mapper;

    {
        mapper = new ObjectMapper();
    }

    public ResultsServlet(ResultFetcher fetcher) {
        this.fetcher = fetcher;
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
        response.getWriter().println(mapper.writeValueAsString(fetcher.dataPoints));
    }
}