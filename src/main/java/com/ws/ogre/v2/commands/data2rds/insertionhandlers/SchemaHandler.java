package com.ws.ogre.v2.commands.data2rds.insertionhandlers;

import java.util.List;
import java.util.Set;

public interface SchemaHandler {
    boolean syncDdls();

    List<String> getJsonMappingsSequence(String theTableName);

    Set<String> getExistingAllTypes();
}