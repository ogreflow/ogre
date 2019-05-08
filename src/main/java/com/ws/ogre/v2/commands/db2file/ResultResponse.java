package com.ws.ogre.v2.commands.db2file;

import com.google.gson.Gson;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;

public class ResultResponse {
    private List<ResultData> data = new ArrayList<>();

    public List<ResultData> getData() {
        return data;
    }

    public void setData(List<ResultData> data) {
        this.data = data;
    }

    public void addData(ResultData data) {
        this.data.add(data);
    }

    public static class ResultData extends LinkedHashMap<String, Object> /* Need to preserve order */ {
    }

    public String toJson() {
        return new Gson().toJson(this);
    }

    @Override
    public String toString() {
        return toJson();
    }
}
