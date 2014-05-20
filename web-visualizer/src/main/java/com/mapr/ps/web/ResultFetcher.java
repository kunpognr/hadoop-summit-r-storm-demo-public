package com.mapr.ps.web;

import com.google.common.collect.Lists;
import com.mapr.ps.web.model.DataPoint;
import org.apache.commons.io.input.Tailer;
import org.apache.commons.io.input.TailerListenerAdapter;
import org.joda.time.DateTime;

import java.io.File;
import java.util.LinkedList;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

public class ResultFetcher {
    static int DATA_POINT_LIMIT = 500;
    final LinkedList<DataPoint> dataPoints = Lists.newLinkedList();
    final ExecutorService executorService = Executors.newSingleThreadExecutor();
    int size = 0;

    public ResultFetcher(String filename) {
        Tailer tailer = new Tailer(new File(filename), new ResultFileListener(), 500);
        executorService.execute(tailer);
    }

    public static DataPoint parseLine(String line) {
        String[] tokens = line.split(";");

        DataPoint dp = new DataPoint();
        dp.setTimestamp(new DateTime(Long.parseLong(tokens[0])));
        dp.setValue(Double.valueOf(tokens[1]));
        // another double    win loss
        // output win / (win + loss )
        return dp;
    }

    public class ResultFileListener extends TailerListenerAdapter {
        @Override
        public void handle(String line) {
            try {
                dataPoints.add(parseLine(line));
            } catch (Exception e) {
                System.err.println(e.getMessage());
                return;
            }


            if (size == DATA_POINT_LIMIT) {
                dataPoints.removeFirst();
            } else {
                size++;
            }
        }
    }
}
