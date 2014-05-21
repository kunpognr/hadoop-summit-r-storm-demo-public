var context= cubism.context()
    .serverDelay(2 * 1000) // Allow 2 sec server delay
    .step(2 * 1000) // Every 2 sec
    .size(940),
    primary = temperature(),
    secondary = primary.shift(- 24 * 60 * 60 * 1000);


d3.select("#chart").call(function(div) {
    div.append("div")
      .attr("class", "axis")
      .call(context.axis().orient("top"));

    div.selectAll(".horizon")
      .data([primary])
      .enter().append("div")
      .attr("class", "horizon")
      .call(context.horizon()
        .height(120)
        .format(d3.format(".2f"))
        .title("Ratio"));

    div.append("div")
      .attr("class", "rule")
      .call(context.rule());
});

context.on("focus", function(i) {
    format = d3.format(".1f");
    d3.selectAll(".horizon .value").style("right", i === null ? null : context.size() - i + "px")
      .text(format(primary.valueAt(Math.floor(i))) + " %");
});


function temperature() {
    return context.metric(function(start, stop, step, callback) {
        d3.json("/data/?start=" + start.toISOString() +
            "&stop=" + stop.toISOString() +
            "&step=" + step, function(data) {
                if(!data) return callback(new Error("unable to load data"));

                // Need to deal with possible gaps in the data
                var plotData = [];
                var recentTS = new Date(start);
                var fillInVal = data[0].temperature;
                data.forEach(function(elt, idx, array) {
                  while(recentTS < elt["ts"]) {
                    // Simulate a point
                    plotData.push(fillInVal);
                    recentTS.setMilliseconds(recentTS.getMilliseconds() + step);
                  }
                  // Add the received data point
                  plotData.push(elt["temperature"]);
                  fillInVal = elt["temperature"];
                  recentTS.setMilliseconds(recentTS.getMilliseconds() + step);
                });

                // Back-fill if required
                while(recentTS < stop) {
                  plotData.push(fillInVal);
                  recentTS.setMilliseconds(recentTS.getMilliseconds() + step);
                }

                // Send the data back to be plotted
                callback(null, plotData );
            });
    });
}
