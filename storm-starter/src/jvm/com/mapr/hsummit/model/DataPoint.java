package com.mapr.hsummit.model;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.databind.annotation.JsonSerialize;
import org.joda.time.DateTime;

public class DataPoint {
    @JsonProperty(value = "ts")
    @JsonSerialize(using = DateTimeJsonSerializer.class)
    DateTime timestamp;

    @JsonProperty(value = "temperature")
    Double value;

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
}
