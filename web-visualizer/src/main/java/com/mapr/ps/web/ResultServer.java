package com.mapr.ps.web;

import org.eclipse.jetty.server.Server;
import org.eclipse.jetty.servlet.DefaultServlet;
import org.eclipse.jetty.servlet.ServletContextHandler;
import org.eclipse.jetty.servlet.ServletHolder;

public class ResultServer {
    public static void main(String[] args) throws Exception {
        System.out.println("Initializing result fetcher");
        final ResultFetcher fetcher = new ResultFetcher("results.txt");

        System.out.println("Initializing server...");
        final ServletContextHandler context =
                new ServletContextHandler(ServletContextHandler.SESSIONS);
        context.setContextPath("/");
        context.setResourceBase("src/main/webapp");

        context.setClassLoader(
                Thread.currentThread().getContextClassLoader()
        );

        context.addServlet(DefaultServlet.class, "/");
        context.addServlet(new ServletHolder(new ResultsServlet(fetcher)), "/data/");

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
        while (true) {
            try {
                server.join();
            } catch (InterruptedException e) {
                System.out.println("Server interrupted!");
            }
        }
    }
}
