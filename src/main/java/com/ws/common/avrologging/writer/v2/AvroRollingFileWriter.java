package com.ws.common.avrologging.writer.v2;

import org.apache.avro.Schema;
import org.apache.avro.file.CodecFactory;
import org.apache.avro.file.DataFileWriter;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.io.DatumWriter;
import org.apache.avro.specific.SpecificDatumWriter;

import java.io.File;
import java.io.IOException;

/**
 */
public class AvroRollingFileWriter<T extends GenericRecord> extends RollingFileWriter<T> {

    private Schema mySchema;

    public AvroRollingFileWriter(Schema theSchema, String theFilenamePattern, String theProgressPostfix, int theMaxRecords, int theMaxAgeS) throws IOException {
        super(theFilenamePattern, theProgressPostfix, theMaxRecords, theMaxAgeS);
        mySchema = theSchema;
    }

    @Override
    protected DataFileWriter<T> getDataFileWriter(File theFile) throws IOException {

        DatumWriter<T> aDatumWriter = new SpecificDatumWriter<>(mySchema);
        DataFileWriter<T> aFileWriter = new DataFileWriter<>(aDatumWriter);
        aFileWriter.setCodec(CodecFactory.snappyCodec());

        aFileWriter.create(mySchema, theFile);

        return aFileWriter;
    }

    @Override
    protected Schema getSchema() {
        return mySchema;
    }


}
