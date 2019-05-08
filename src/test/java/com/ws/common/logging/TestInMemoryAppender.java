package com.ws.common.logging;

import org.apache.log4j.Appender;
import org.apache.log4j.Layout;
import org.apache.log4j.Level;
import org.apache.log4j.spi.ErrorHandler;
import org.apache.log4j.spi.Filter;
import org.apache.log4j.spi.LoggingEvent;

import java.util.ArrayList;
import java.util.List;

public class TestInMemoryAppender implements Appender {
    private List<String> myLogRows;
    private List<Level> myLogRowLevels;
    private List<Throwable> myThrowables;
    private List<Object> myLogTraceIds;

    public TestInMemoryAppender() {
        myLogRows = new ArrayList<String>();
        myLogRowLevels = new ArrayList<Level>();
        myThrowables = new ArrayList<Throwable>();
        myLogTraceIds = new ArrayList<Object>();

    }

    public List<String> getLogRows() {
        return myLogRows;
    }

    public List<Level> getLogRowLevels() {
        return myLogRowLevels;
    }

    public List<Throwable> getThrowables() {
        return myThrowables;
    }

    public List<Object> getLogTraceIds() {
        return myLogTraceIds;
    }

    @Override
    public void addFilter(Filter newFilter) {
        //To change body of implemented methods use File | Settings | File Templates.
    }

    @Override
    public Filter getFilter() {
        return null;  //To change body of implemented methods use File | Settings | File Templates.
    }

    @Override
    public void clearFilters() {
        //To change body of implemented methods use File | Settings | File Templates.
    }

    @Override
    public void close() {
        //To change body of implemented methods use File | Settings | File Templates.
    }

    @Override
    public void doAppend(LoggingEvent event) {
        // System.out.println("event = " + event);
        myLogRows.add(event.getMessage().toString());
        myLogRowLevels.add(event.getLevel());
        myLogTraceIds.add(event.getMDC("logtraceid"));
        if (event.getThrowableInformation() != null) {
            myThrowables.add(event.getThrowableInformation().getThrowable());
        }
        // System.out.println("event = " + event);
    }

    @Override
    public String getName() {
        return null;  //To change body of implemented methods use File | Settings | File Templates.
    }

    @Override
    public void setErrorHandler(ErrorHandler errorHandler) {
        //To change body of implemented methods use File | Settings | File Templates.
    }

    @Override
    public ErrorHandler getErrorHandler() {
        return null;  //To change body of implemented methods use File | Settings | File Templates.
    }

    @Override
    public void setLayout(Layout layout) {
        //To change body of implemented methods use File | Settings | File Templates.
    }

    @Override
    public Layout getLayout() {
        return null;  //To change body of implemented methods use File | Settings | File Templates.
    }

    @Override
    public void setName(String name) {
        //To change body of implemented methods use File | Settings | File Templates.
    }

    @Override
    public boolean requiresLayout() {
        return false;  //To change body of implemented methods use File | Settings | File Templates.
    }
}
