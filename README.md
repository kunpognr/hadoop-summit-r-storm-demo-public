hadoop-summit-storm-demo
========================

Allen Day's Hadoop Summit Twitter Storm demo


web-visualizer/
---------------

Contains a embedded jetty server that serves a page. The web page runs Cubism.js (on top of D3.js) and periodically pulls new data from the server (through XHR).

The server responds on /data/ with the last 500 data points, that are read tailing a specified file. The datapoints are served in a JSON format.