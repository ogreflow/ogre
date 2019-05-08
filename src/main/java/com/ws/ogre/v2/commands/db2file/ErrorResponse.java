package com.ws.ogre.v2.commands.db2file;

import com.google.gson.Gson;

public class ErrorResponse {
    private boolean success = false;

    private String error;

    public ErrorResponse() {
    }

    public ErrorResponse(String theError) {
        setError(theError);
    }

    public boolean isSuccess() {
        return success;
    }

    public void setSuccess(boolean success) {
        this.success = success;
    }

    public String getError() {
        return error;
    }

    public void setError(String error) {
        this.error = error;
    }

    public String toJson() {
        return new Gson().toJson(this);
    }

    @Override
    public String toString() {
        return toJson();
    }
}
