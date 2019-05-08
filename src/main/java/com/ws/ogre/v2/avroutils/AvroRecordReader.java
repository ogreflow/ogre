package com.ws.ogre.v2.avroutils;

import org.apache.avro.file.DataFileStream;
import org.apache.avro.generic.GenericDatumReader;
import org.apache.avro.generic.GenericRecord;

import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.List;

public class AvroRecordReader extends DataFileStream<GenericRecord> {
    public AvroRecordReader(InputStream theIn) throws IOException {
        super(theIn, new GenericDatumReader<GenericRecord>());
    }

    public List<List<Object>> readValues(List<String> theJsonPaths) {
        AvroPaths somePaths = new AvroPathParser(getSchema()).getPaths(theJsonPaths);

        List<List<Object>> someValues = new ArrayList<>();
        while (hasNext()) {
            GenericRecord aRecord = next();

            List<Object> aValues = new ArrayList<>();
            for (AvroPath aPath : somePaths) {
                aValues.add(aPath.extract(aRecord));
            }

            someValues.add(aValues);
        }

        return someValues;
    }
}