package com.examplesql.talend.components.output;

import org.talend.sdk.component.api.record.Record;

public class Reject {

    private final String msg;

    private String sqlState;

    private Integer errorCode;

    private final Record record;

    public Reject(final String msg, final Record record)
    {
        this(msg, "",0,record);
    }

    public Reject(final String msg, String sqlState, Integer errorCode, final Record record)
    {
        this.msg = msg;
        this.sqlState = sqlState;
        this.errorCode = errorCode;
        this.record = record;
    }
    @Override
    public String toString() {
        return "{\"sqlState\": \"" + sqlState + "\", \"errorCode\":" + errorCode + ", \"msg\": \"" + msg + "\", \"record\":"
                + record + "}";
    }
}