package com.ws.common.avrologging.writer;

import com.ws.common.logging.Logger;
import org.apache.avro.Schema;
import org.apache.avro.file.CodecFactory;
import org.apache.avro.file.DataFileWriter;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.io.DatumWriter;
import org.apache.avro.specific.SpecificDatumWriter;

import java.io.File;
import java.io.IOException;

/**
 * Class writing avro records to a specific file.
 */
public class AvroFileWriter<T extends GenericRecord> {

    public static final Logger ourLog = Logger.getLogger();

    private DataFileWriter<T> myWriter = null;

    public AvroFileWriter(String theFileName, Schema theSchema) throws IOException {

        ourLog.debug("Create new AVRO file: %s", theFileName);

        createDir(theFileName);

        DatumWriter<T> aWriter = new SpecificDatumWriter<>(theSchema);
        myWriter = new DataFileWriter<>(aWriter);
        myWriter.setCodec(CodecFactory.snappyCodec());

        myWriter.create(theSchema, new File(theFileName));
    }

    @SuppressWarnings("all")
    private void createDir(String theFilename) {
        int aPos = theFilename.lastIndexOf('/');

        String aDir = theFilename.substring(0, aPos);

        new File(aDir).mkdirs();
    }

    public synchronized void write(T theEntry) throws IOException {
        myWriter.append(theEntry);
    }

    public synchronized void close() throws IOException {
        if (myWriter == null) {
            return;
        }

        ourLog.debug("Closing file");

        myWriter.close();
        myWriter = null;
    }


}
