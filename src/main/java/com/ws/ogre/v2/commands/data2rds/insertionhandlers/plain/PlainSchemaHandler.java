package com.ws.ogre.v2.commands.data2rds.insertionhandlers.plain;

import com.ws.ogre.v2.commands.data2rds.insertionhandlers.SchemaHandler;

import java.util.Collections;
import java.util.List;
import java.util.Set;

public class PlainSchemaHandler implements SchemaHandler {
    public PlainSchemaHandler() {
    }

    @Override
    public boolean syncDdls() {
        return true;
    }

    @Override
    public List<String> getJsonMappingsSequence(String theTableName) {
        return Collections.emptyList();
    }

    @Override
    public Set<String> getExistingAllTypes() {
        return Collections.emptySet();
    }
}