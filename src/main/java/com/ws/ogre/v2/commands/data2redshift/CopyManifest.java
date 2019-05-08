package com.ws.ogre.v2.commands.data2redshift;

import java.util.ArrayList;
import java.util.List;

/*
{
  "entries": [
    {"url":"s3://mybucket-alpha/custdata.1","mandatory":true},
    {"url":"s3://mybucket-alpha/custdata.2","mandatory":true},
    {"url":"s3://mybucket-beta/custdata.1","mandatory":false}
  ]
}
 */
public class CopyManifest {

    public List<Entry> entries = new ArrayList<>();


    public static class Entry {

        public String url;
        public boolean mandatory;

        @Override
        public String toString() {
            return "Mapping{" +
                    "url='" + url + '\'' +
                    ", mandatory=" + mandatory +
                    '}';
        }

    }

    @Override
    public String toString() {
        return "CopyManifest{" +
                "entries=" + entries +
                '}';
    }
}
