package com.ws.ogre.v2.avroutils;

import org.apache.avro.Schema;

import java.util.Arrays;
import java.util.List;

/**
 * Parses an avro schema and extracts paths to maps and arrays.
 */
public class AvroPathParser {

    private AvroPaths myArrayPaths = new AvroPaths();
    private AvroPaths myMapPaths = new AvroPaths();


    public AvroPathParser(Schema theSchema) {
        parse(theSchema, "$");
    }

    private void parse(Schema theSchema, String thePath) {

        switch (theSchema.getType()) {

            case UNION:
                for (Schema aType : theSchema.getTypes()) {
                    parse(aType, thePath);
                }
                break;

            case RECORD:

                for (Schema.Field aField : theSchema.getFields()) {

                    String aFieldName = aField.name();
                    String aPath = thePath + "." + aFieldName;

                    Schema aSchema = aField.schema();
                    parse(aSchema, aPath);
                }
                break;

            case ARRAY:

                myArrayPaths.add(new AvroPath(thePath));

                // Removed, we do only support first level arrays
//                Schema aType = theSchema.getElementType();
//                parse(aType, thePath);
                break;

            case MAP:

                myMapPaths.add(new AvroPath(thePath));

                // Removed, we do only support first level maps
//                Schema aType = theSchema.getElementType();
//                parse(aType, thePath);
                break;
        }
    }

    public AvroPaths getPaths(String ... theJsonPaths) {
        return getPaths(Arrays.asList(theJsonPaths));
    }

    public AvroPaths getPaths(List<String> theJsonPaths) {

        AvroPaths aPaths = new AvroPaths();

        for (String aJsonPath : theJsonPaths) {
            aPaths.add(new AvroPath(aJsonPath));
        }

        return aPaths;
    }

    public AvroPaths getArrayPaths() {
        return myArrayPaths;
    }

    public AvroPaths getMapPaths() {
        return myMapPaths;
    }
}
