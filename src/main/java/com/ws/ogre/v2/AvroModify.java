package com.ws.ogre.v2;

import com.ws.ogre.v2.aws.S3Client;
import com.ws.ogre.v2.aws.S3Url;
import com.ws.ogre.v2.avroutils.AvroPath;
import org.apache.avro.Schema;
import org.apache.avro.file.CodecFactory;
import org.apache.avro.file.DataFileStream;
import org.apache.avro.file.DataFileWriter;
import org.apache.avro.generic.GenericArray;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericDatumReader;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.specific.SpecificDatumWriter;

import java.io.*;
import java.util.HashMap;
import java.util.Map;

/**
 * Class that interactively can navigate an AVRO file in S3 or on file and modify its content.
 *
 * This is very useful to remove bad values if problems to load file into a Redshift.
 */
public class AvroModify {

    static BufferedReader ourIn = new BufferedReader(new InputStreamReader(System.in));

    /* Just hide away some generics ugliness */
    static class MyAvroRecordReader extends DataFileStream<GenericRecord> {
        public MyAvroRecordReader(InputStream theIn) throws IOException {
            super(theIn, new GenericDatumReader<GenericRecord>());
        }
    }

    /* Just hide away some generics ugliness */
    static class MyAvroRecordWriter extends DataFileWriter<GenericRecord> {
        public MyAvroRecordWriter(Schema theSchema, File theFile) throws IOException {
            super(new SpecificDatumWriter<GenericRecord>(theSchema));
            setCodec(CodecFactory.snappyCodec());
            create(theSchema, theFile);
        }
    }

    private static double getNumberInput(String theMessage) throws Exception {
        return new Double(getInput(theMessage));
    }

    private static String getInput(String theMessage) throws Exception {
        System.out.println(theMessage);
        return ourIn.readLine();
    }

    private static void printField(Schema.Field theField, GenericRecord theRecord) {
        Schema aSchema = theField.schema();
        int aPos = theField.pos();
        String aName = theField.name();
        boolean isNullable = aSchema.getType() == Schema.Type.UNION;
        Schema.Type aType = aSchema.getType();

        if (isNullable) {
            for (Schema anUnionSchema : aSchema.getTypes()) {
                if (anUnionSchema.getType() == Schema.Type.NULL) {
                    continue;
                }

                aType = anUnionSchema.getType();
            }
        }

        System.out.printf("| %3s | %-20s | %-10s | %s\n", aPos, aName, aType, theRecord.get(aPos));
    }

    private static void changeRecord(GenericRecord theRecord) throws Exception {

        // Create a map<pos, field + value>
        // stin select pos to change, can traverse into unions, maps, arrays and records in recursive order until
        // value found to change, replace with new value and continue

        while (true) {

            try {
                System.out.println("+-----+----------------------+------------+---------------------------");
                System.out.println("| Pos | Field                | Type       | Value");
                System.out.println("+-----+----------------------+------------+---------------------------");

                Map<Integer, Schema.Field> aFields = new HashMap<>();

                Schema aSchema = theRecord.getSchema();

                for (Schema.Field aField : aSchema.getFields()) {
                    printField(aField, theRecord);

                    aFields.put(aField.pos(), aField);
                }

                System.out.println("+-----+----------------------+------------+---------------------------");
                System.out.println("|   B | Go back");
                System.out.println("+-----+---------------------------------------------------------------");
                System.out.println("");

                String anInput = getInput("Select (or drill into) the field to update:");

                if (!anInput.matches("\\d*|[bB]")) {
                    System.out.println("Invalid input, retry!");
                    continue;
                }

                if ("B".equalsIgnoreCase(anInput)) {
                    return;
                }

                int aPos = new Integer(anInput);

                setValue(new AvroRecordValue(theRecord, aFields.get(aPos)));

            } catch (Exception e) {
                System.out.println("Failure: " + e.getMessage());
            }
        }
    }

    private static void changeArray(GenericArray theArray) throws Exception {

        while (true) {

            try {
                System.out.println("+-----+---------------------------");
                System.out.println("| Idx | Value");
                System.out.println("+-----+---------------------------");

                for (int i = 0; i < theArray.size(); i++) {
                    System.out.printf("| %3s | %s\n", i, theArray.get(i));
                }

                System.out.println("+-----+---------------------------");
                System.out.println("|   A | Add new");
                System.out.println("|   R | Remove");
                System.out.println("|   B | Go back");
                System.out.println("+-----+---------------------------");
                System.out.println("");

                String anInput = getInput("Select index of value to update:");

                if (!anInput.matches("\\d*|[aArRbB]")) {
                    System.out.println("Invalid input, retry!");
                    continue;
                }

                if ("B".equalsIgnoreCase(anInput)) {
                    return;
                }

                if ("R".equalsIgnoreCase(anInput)) {
                    int anIdx = new Integer(getInput("Index of value to remove:"));
                    theArray.remove(anIdx);
                    continue;
                }

                if ("A".equalsIgnoreCase(anInput)) {
                    int anIdx = new Integer(getInput("Index where to add value:"));
                    theArray.add(anIdx, createObject(theArray.getSchema().getElementType()));
                    setValue(new AvroArrayValue(theArray, anIdx));
                    continue;
                }

                int aPos = new Integer(anInput);

                setValue(new AvroArrayValue(theArray, aPos));

            } catch (Exception e) {
                System.out.println("Failure: " + e.getMessage());
            }
        }
    }

    private static void changeMap(HashMap<String,Object> theMap, Schema theMapSchema) throws Exception {

        while (true) {

            try {
                System.out.println("+-----+------------------------+---------------------------");
                System.out.println("| Idx | Key                    | Value");
                System.out.println("+-----+------------------------+---------------------------");

                Map<String, String> aIdxKeyMap = new HashMap<>();

                int anIdx = 1;
                for (String aKey : theMap.keySet()) {
                    aIdxKeyMap.put("" + anIdx, aKey);
                    System.out.printf("| %-3s | %23s | %s\n", anIdx++, aKey, theMap.get(aKey));
                }

                System.out.println("+-----+---------------------------");
                System.out.println("|   A | Add new");
                System.out.println("|   R | Remove");
                System.out.println("|   B | Go back");
                System.out.println("+-----+---------------------------");
                System.out.println("");

                String anInput = getInput("Select command or index of key to modify:");

                if (!anInput.matches("\\d*|[aArRbB]")) {
                    System.out.println("Invalid input, retry!");
                    continue;
                }

                if ("B".equalsIgnoreCase(anInput)) {
                    return;
                }

                if ("R".equalsIgnoreCase(anInput)) {
                    String aRemoveIdx = getInput("Index of key to remove: ");
                    String aKey = aIdxKeyMap.get(aRemoveIdx);
                    if (aKey == null) {
                        System.out.println("Invalid input, retry!");
                        continue;
                    }
                    theMap.remove(aKey);
                    continue;
                }

                if ("A".equalsIgnoreCase(anInput)) {
                    String aKey = getInput("Key to add:");
                    theMap.put(aKey, createObject(theMapSchema.getValueType()));
                    setValue(new AvroMapValue(theMap, aKey, theMapSchema));
                    continue;
                }

                String aKey = aIdxKeyMap.get(anInput);
                if (aKey == null) {
                    System.out.println("Invalid input, retry!");
                    continue;
                }

                setValue(new AvroMapValue(theMap, aKey, theMapSchema));

            } catch (Exception e) {
                System.out.println("Failure: " + e.getMessage());
            }
        }
    }

    public static void setValue(AvroValue theValue) throws Exception {

        if (theValue.isNullable()) {
            boolean aNullify = getInput("Nullify (y/n):").equalsIgnoreCase("y");

            if (aNullify) {
                theValue.set(null);
                return;
            }
        }

        switch (theValue.getType()) {
            case LONG:
            case INT:
            case DOUBLE:
            case FLOAT:
                theValue.set(getNumberInput("Value:"));
                break;

            case STRING:
                theValue.set(getInput("Value:"));
                break;

            case BOOLEAN:
                theValue.set(getInput("Value:").equalsIgnoreCase("true"));
                break;

            case UNION:
                throw new RuntimeException("Should have been removed in AvroValue");

            case RECORD:
                changeRecord((GenericRecord) theValue.get());
                break;

            case ARRAY:
                changeArray((GenericData.Array) theValue.get());
                break;

            case MAP:
                changeMap((HashMap<String, Object>) theValue.get(), theValue.getValueSchema());
                break;

            case ENUM:
            case FIXED:
            case BYTES:
            case NULL:
        }

    }

    public static Object createObject(Schema theSchema) {

        switch (theSchema.getType()) {
            case RECORD:
                return new GenericData.Record(theSchema);

            case ARRAY:
                return new GenericData.Array(theSchema, null);

            case STRING:
                return "";

            case INT:
                return -1;

            case LONG:
                return -1l;

            case FLOAT:
                return -1f;

            case DOUBLE:
                return -1d;

            case BOOLEAN:
                return false;

            case BYTES:
            case FIXED:
            case ENUM:
            case MAP:
            case UNION:
            case NULL:
            default:
                throw new RuntimeException("Not implemented: " + theSchema.getType());

        }
    }

    public interface AvroValue {
        boolean isNullable();
        void set(Object theValue);
        Schema.Type getType();
        Object get();
        Schema getValueSchema();
    }

    private static class AvroRecordValue implements AvroValue {
        GenericRecord myRecord;
        Schema.Field myField;
        boolean iAmNullable;
        Schema.Type myType;

        public AvroRecordValue(GenericRecord theRecord, Schema.Field theField) {
            myRecord = theRecord;
            myField = theField;

            iAmNullable = theField.schema().getType() == Schema.Type.UNION;

            myType = theField.schema().getType();

            if (myType == Schema.Type.UNION) {
                for (Schema aSchema : theField.schema().getTypes()) {

                    myType = aSchema.getType();

                    if (myType != Schema.Type.NULL) {
                        break;
                    }
                }
            }
        }

        @Override
        public Schema getValueSchema() {
            return myField.schema();
        }

        @Override
        public boolean isNullable() {
            return iAmNullable;
        }

        @Override
        public void set(Object theValue) {
            myRecord.put(myField.pos(), theValue);
        }

        @Override
        public Schema.Type getType() {
            return myType;
        }

        @Override
        public Object get() {
            return myRecord.get(myField.pos());
        }
    }

    private static class AvroArrayValue implements AvroValue {
        GenericArray myArray;
        int myIndex;
        boolean iAmNullable;
        Schema.Type myType;

        public AvroArrayValue(GenericArray theArray, int theIndex) {
            myArray = theArray;
            myIndex = theIndex;

            iAmNullable = theArray.getSchema().getType() == Schema.Type.UNION;

            myType = theArray.getSchema().getElementType().getType();

            if (myType == Schema.Type.UNION) {
                for (Schema aSchema : theArray.getSchema().getTypes()) {

                    myType = aSchema.getType();

                    if (myType != Schema.Type.NULL) {
                        break;
                    }
                }
            }
        }

        @Override
        public Schema getValueSchema() {
            return myArray.getSchema();
        }

        @Override
        public boolean isNullable() {
            return iAmNullable;
        }

        @Override
        public void set(Object theValue) {
            myArray.set(myIndex, theValue);
        }

        @Override
        public Schema.Type getType() {
            return myType;
        }

        @Override
        public Object get() {
            return myArray.get(myIndex);
        }
    }

    private static class AvroMapValue implements AvroValue {
        HashMap<String, Object> myMap;
        String myKey;
        Schema myValueSchema;
        Schema.Type myType;

        public AvroMapValue(HashMap<String, Object> theMap, String theKey, Schema theMapSchema) {
            myMap = theMap;
            myKey = theKey;

            myType = theMapSchema.getValueType().getType();
            myValueSchema = theMapSchema.getValueType();

            if (myType == Schema.Type.UNION) {
                throw new UnsupportedOperationException("MAP unions is not implemented");
            }
        }

        @Override
        public Schema getValueSchema() {
            return myValueSchema;
        }

        @Override
        public boolean isNullable() {
            return false;
        }

        @Override
        public void set(Object theValue) {
            myMap.put(myKey, theValue);
        }

        @Override
        public Schema.Type getType() {
            return myType;
        }

        @Override
        public Object get() {
            return myMap.get(myKey);
        }
    }


    public static void main(String[] theArgs) throws Exception {

        String anAvroLocation;

        if (theArgs.length == 0) {
            anAvroLocation = getInput("The S3 URL or path to Avro file:");
        } else {
            anAvroLocation = theArgs[0];
        }

        File anOrgFile = null;
        File anModFile = null;

        if (!anAvroLocation.startsWith("s3:")) {
            anOrgFile = new File(anAvroLocation);
            anModFile = new File(anAvroLocation + ".mod");

        } else {
            S3Url anUrl = new S3Url(anAvroLocation);

            String aKey;
            String aSecret;

            if (theArgs.length == 0) {
                aKey = getInput("The Access Key Id: ");
                aSecret = getInput("The Secret Key: ");
            } else {
                aKey = theArgs[1];
                aSecret = theArgs[2];
            }

            S3Client aClient = new S3Client(aKey, aSecret);

            anOrgFile = File.createTempFile(anUrl.bucket, "apa");
            anModFile = File.createTempFile(anUrl.bucket, "apa" + ".mod");

            aClient.getObjectToFile(anUrl.bucket, anUrl.key, anOrgFile);
        }

        MyAvroRecordReader aReader = new MyAvroRecordReader(new FileInputStream(anOrgFile));
        MyAvroRecordWriter aWriter = new MyAvroRecordWriter(aReader.getSchema(), anModFile);

        String aSearchByLineNbr = getInput("Edit line number (Y/N): ").toLowerCase();

        GenericRecord aRecord = null;

        if (aSearchByLineNbr.equals("y")) {

            int aLineNbr;

            if (theArgs.length == 0) {
                aLineNbr = (int) getNumberInput("Line number to update: ");
            } else {
                aLineNbr = new Integer(theArgs[theArgs.length - 1]);
            }

            int aLine = 0;

            while (aReader.hasNext() && ++aLine < aLineNbr) {
                aWriter.append(aReader.next());
            }

            aRecord = aReader.next();

            if (aReader.hasNext()) {
                changeRecord(aRecord);
                aWriter.append(aRecord);
            }

        } else {

            String aJsonPath = getInput("The json path to search: ");

            AvroPath aPath = new AvroPath(aJsonPath);

            String aValue = getInput("The value to search for (<null> for null): ");

            while (aReader.hasNext()) {

                aRecord = aReader.next();

                Object anAvroValue = aPath.extract(aRecord);

                System.out.println(anAvroValue);


                if (aValue.equals("<null>")) {
                    if (anAvroValue != null) {
                        aWriter.append(aRecord);
                        continue;
                    }
                } else {
                    if (!aValue.equals(anAvroValue.toString())) {
                        aWriter.append(aRecord);
                        continue;
                    }
                }


                changeRecord(aRecord);
                aWriter.append(aRecord);

                if (getInput("Continue search (Y/N): ").toLowerCase().equals("n")) {
                    break;
                }
            }
        }

        while (aReader.hasNext()) {
            aWriter.append(aReader.next());
        }

        aWriter.close();

        System.out.println("Written modified file to: " + anModFile.getAbsolutePath());
    }

}
