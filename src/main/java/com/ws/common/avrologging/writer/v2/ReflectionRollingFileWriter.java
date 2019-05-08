package com.ws.common.avrologging.writer.v2;

import org.apache.avro.Schema;
import org.apache.avro.file.CodecFactory;
import org.apache.avro.file.DataFileWriter;
import org.apache.avro.io.DatumWriter;
import org.apache.avro.reflect.ReflectData;
import org.apache.avro.reflect.ReflectDatumWriter;

import java.io.File;
import java.io.IOException;

/**
 */
public class ReflectionRollingFileWriter<T> extends RollingFileWriter<T> {

    private Schema mySchema;

    public ReflectionRollingFileWriter(Class<T> theClass, String theFilenamePattern, String theProgressPostfix, int theMaxRecords, int theMaxAgeS) throws IOException {
        super(theFilenamePattern, theProgressPostfix, theMaxRecords, theMaxAgeS);

        mySchema = ReflectData.get().getSchema(theClass);
    }

    @Override
    protected DataFileWriter<T> getDataFileWriter(File theFile) throws IOException {

        DatumWriter<T> aDatumWriter = new ReflectDatumWriter<>(mySchema);
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
