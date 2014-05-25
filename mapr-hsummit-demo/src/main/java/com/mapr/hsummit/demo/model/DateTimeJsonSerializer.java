package com.mapr.hsummit.demo.model;

import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonSerializer;
import com.fasterxml.jackson.databind.SerializerProvider;
import org.joda.time.DateTime;
import org.joda.time.format.DateTimeFormat;
import org.joda.time.format.DateTimeFormatter;
import org.joda.time.format.DateTimeFormatterBuilder;
import org.joda.time.format.ISODateTimeFormat;

import java.io.IOException;

public class DateTimeJsonSerializer extends JsonSerializer<DateTime> {

    // like 2014-05-19 12:32:02 UTC
    private static DateTimeFormatter formatter = DateTimeFormat.forPattern("yyyy-MM-dd HH:mm");

    @Override
    public void serialize(DateTime value, JsonGenerator gen,
                          SerializerProvider arg2)
            throws IOException, JsonProcessingException {
        gen.writeString(formatter.print(value));
    }
}