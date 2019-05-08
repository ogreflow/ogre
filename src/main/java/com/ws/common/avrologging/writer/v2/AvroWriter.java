package com.ws.common.avrologging.writer.v2;

import java.io.Closeable;

/**
 * Avro writer contract.
 */
public abstract class AvroWriter<T> implements Closeable {

    /**
     * Closing all underlying writers.
     */
    public abstract void close();

    /**
     * Writes record to Avro container file.
     *
     * @param theTimestamp The timestamp for record used to sort it into the correct avro container file
     */
    public abstract void write(long theTimestamp, T theRecord) throws Exception;


}
