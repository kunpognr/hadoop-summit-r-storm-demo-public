package com.mapr.hsummit.demo.model;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonUnwrapped;
import com.fasterxml.jackson.databind.annotation.JsonSerialize;
import org.joda.time.DateTime;

import java.util.HashMap;

public class DataPoint {
    @JsonProperty(value = "date")
    @JsonSerialize(using = DateTimeJsonSerializer.class)
    DateTime timestamp;

    @JsonProperty(value = "wp")
    Double value;

    @JsonProperty(value = "changepoint level")
    Double level;

    public DateTime getTimestamp() {
        return timestamp;
    }

    public void setTimestamp(DateTime timestamp) {
        this.timestamp = timestamp;
    }

    public Double getValue() {
        return value;
    }

    public void setValue(Double value) {
        this.value = value;
    }

    public Double getLevel() {return level; }

    public void setLevel(Double level) { this.level = level; }
}
