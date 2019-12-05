package com.ws.ogre.v2.datafile;

import com.google.gson.Gson;
import com.google.gson.annotations.SerializedName;
import com.ws.ogre.v2.aws.S3BetterUrl;
import com.ws.ogre.v2.datafile.DataFileHandler.*;

import java.util.List;

import java.util.ArrayList;

/**
 */
public class DataFileManifest {

    @SerializedName("file")
    private String myFile;

    @SerializedName("includes")
    private List<String> myIncludes = new ArrayList<>();

    public S3BetterUrl getFile() {
        return new S3BetterUrl(myFile);
    }

    public void setFile(S3BetterUrl theFile) {
        myFile = theFile.toString();
    }

    public void setIncludes(DataFiles theFiles) {
        for (DataFile aFile : theFiles) {
            myIncludes.add(aFile.url.toString());
        }
    }

    public DataFiles getIncludes() {
        DataFiles aFiles = new DataFiles();

        for (String anUrl : myIncludes) {
            aFiles.add(new DataFile(new S3BetterUrl(anUrl)));
        }

        return aFiles;
    }

    public DataFileManifest deserialize(String theJson) {
        return new Gson().fromJson(theJson, DataFileManifest.class);
    }

    public String serialize() {
        return new Gson().toJson(this);
    }
}
