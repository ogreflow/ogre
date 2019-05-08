package com.ws.ogre.v2.avroutils;

import org.apache.avro.generic.GenericRecord;

public class AvroPath {

    public String path;
    public String field;
    public String[] subpaths;

    public AvroPath(String theJsonPath) {

        String[] aParts = theJsonPath.substring(2).split("\\.");

        field = aParts[aParts.length - 1];
        path = theJsonPath;
        subpaths = new String[aParts.length - 1];

        System.arraycopy(aParts, 0, subpaths, 0, subpaths.length);
    }

    public Object extract(GenericRecord theRecord) {

        for (String aPath : this.subpaths) {
            theRecord = (GenericRecord) theRecord.get(aPath);

            if (theRecord == null) {
                return null;
            }
        }

        return theRecord.get(this.field);
    }

    public String getJsonPath() {
        return path;
    }

    @Override
    public String toString() {
        return "AvroPath{" +
                "path='" + path + '\'' +
                '}';
    }
}