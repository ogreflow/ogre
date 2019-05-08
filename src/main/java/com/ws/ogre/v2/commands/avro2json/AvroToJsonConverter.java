package com.ws.ogre.v2.commands.avro2json;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericContainer;
import org.apache.avro.generic.GenericEnumSymbol;
import org.apache.avro.generic.GenericFixed;
import org.apache.avro.generic.IndexedRecord;

import java.nio.ByteBuffer;
import java.util.Collection;
import java.util.Iterator;
import java.util.Map;

/**
 * Stolen from the Avro project.
 */
public class AvroToJsonConverter {


    public static String toString(Object datum) {
        StringBuilder buffer = new StringBuilder();
        toString(datum, buffer);
        return buffer.toString();
    }

    public static void toString(Object datum, StringBuilder buffer) {
        int bytes;
        Iterator i$;
        if(isRecord(datum)) {
            buffer.append("{");
            bytes = 0;
            Schema i = getRecordSchema(datum);
            i$ = i.getFields().iterator();
            boolean separator = false;

            while(i$.hasNext()) {
                Schema.Field entry = (Schema.Field)i$.next();

                Object field = getField(datum, entry.name(), entry.pos());

                if (field == null) {
                    continue;
                }

                if(separator) {
                    buffer.append(", ");
                }

                toString(entry.name(), buffer);
                buffer.append(": ");
                toString(field, buffer);

                separator = true;
                ++bytes;
            }

            buffer.append("}");
        } else if(isArray(datum)) {
            Collection var9 = (Collection)datum;
            buffer.append("[");
            long var10 = (long)(var9.size() - 1);
            int var14 = 0;
            Iterator i$1 = var9.iterator();

            while(i$1.hasNext()) {
                Object element = i$1.next();
                toString(element, buffer);
                if((long)(var14++) < var10) {
                    buffer.append(", ");
                }
            }

            buffer.append("]");
        } else if(isMap(datum)) {
            buffer.append("{");
            bytes = 0;
            Map var11 = (Map)datum;
            i$ = var11.entrySet().iterator();

            while(i$.hasNext()) {
                Map.Entry var15 = (Map.Entry)i$.next();
                toString(var15.getKey(), buffer);
                buffer.append(": ");
                toString(var15.getValue(), buffer);
                ++bytes;
                if(bytes < var11.size()) {
                    buffer.append(", ");
                }
            }

            buffer.append("}");
        } else if(!isString(datum) && !isEnum(datum)) {
            if(isBytes(datum)) {
                buffer.append("{\"bytes\": \"");
                ByteBuffer var12 = (ByteBuffer)datum;

                for(int var13 = var12.position(); var13 < var12.limit(); ++var13) {
                    buffer.append((char)var12.get(var13));
                }

                buffer.append("\"}");
            } else if((!(datum instanceof Float) || !((Float)datum).isInfinite() && !((Float)datum).isNaN()) && (!(datum instanceof Double) || !((Double)datum).isInfinite() && !((Double)datum).isNaN())) {
                buffer.append(datum);
            } else {
                buffer.append("\"");
                buffer.append(datum);
                buffer.append("\"");
            }
        } else {
            buffer.append("\"");
            writeEscapedString(datum.toString(), buffer);
            buffer.append("\"");
        }

    }
    protected static boolean isArray(Object datum) {
        return datum instanceof Collection;
    }

    protected static boolean isRecord(Object datum) {
        return datum instanceof IndexedRecord;
    }

    protected static Schema getRecordSchema(Object record) {
        return ((GenericContainer)record).getSchema();
    }

    protected static boolean isEnum(Object datum) {
        return datum instanceof GenericEnumSymbol;
    }

    protected static Schema getEnumSchema(Object enu) {
        return ((GenericContainer)enu).getSchema();
    }

    protected static boolean isMap(Object datum) {
        return datum instanceof Map;
    }

    protected static boolean isFixed(Object datum) {
        return datum instanceof GenericFixed;
    }

    protected static Schema getFixedSchema(Object fixed) {
        return ((GenericContainer)fixed).getSchema();
    }

    protected static boolean isString(Object datum) {
        return datum instanceof CharSequence;
    }

    protected static boolean isBytes(Object datum) {
        return datum instanceof ByteBuffer;
    }

    protected static boolean isInteger(Object datum) {
        return datum instanceof Integer;
    }

    protected static boolean isLong(Object datum) {
        return datum instanceof Long;
    }

    protected static boolean isFloat(Object datum) {
        return datum instanceof Float;
    }

    protected static boolean isDouble(Object datum) {
        return datum instanceof Double;
    }

    protected static boolean isBoolean(Object datum) {
        return datum instanceof Boolean;
    }


    public static Object getField(Object record, String name, int position) {
        return ((IndexedRecord)record).get(position);
    }


    private static void writeEscapedString(String string, StringBuilder builder) {
        for(int i = 0; i < string.length(); ++i) {
            char ch = string.charAt(i);
            switch(ch) {
                case '\b':
                    builder.append("\\b");
                    break;
                case '\t':
                    builder.append("\\t");
                    break;
                case '\n':
                    builder.append("\\n");
                    break;
                case '\f':
                    builder.append("\\f");
                    break;
                case '\r':
                    builder.append("\\r");
                    break;
                case '\"':
                    builder.append("\\\"");
                    break;
                case '\\':
                    builder.append("\\\\");
                    break;
                default:
                    if((ch < 0 || ch > 31) && (ch < 127 || ch > 159) && (ch < 8192 || ch > 8447)) {
                        builder.append(ch);
                    } else {
                        String hex = Integer.toHexString(ch);
                        builder.append("\\u");

                        for(int j = 0; j < 4 - hex.length(); ++j) {
                            builder.append('0');
                        }

                        builder.append(hex.toUpperCase());
                    }
            }
        }

    }
}
