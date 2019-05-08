package com.ws.ogre.v2.commands.db2avro;

public enum StorageType {

    AVRO("avro"),
    CSV("csv"),
    TSV("tsv");

    public static StorageType fromValue(String theStorageTypeId) throws IllegalArgumentException {
        for (StorageType aStorageType : StorageType.values()) {
            if (aStorageType.myTypeId.equals(theStorageTypeId)) {
                return aStorageType;
            }
        }

        throw new IllegalArgumentException("Cannot create enum from " + theStorageTypeId + " value!");
    }

    private final String myTypeId;

    StorageType(String theTypeId) {
        myTypeId = theTypeId;
    }

    public String getTypeId() {
        return myTypeId;
    }
}
