package com.ws.ogre.v2.db;

public class SqlUtils {

    public static String removeComments(String theString) {

        String aString = "";

        boolean inQuote = false;
        boolean inCommentSlash = false;
        boolean inCommentAstrix = false;

        while (theString.length() > 0) {

            String aPart = theString;

            theString = theString.substring(1);

            if (inQuote) {
                aString += aPart.charAt(0);

                if (aPart.startsWith("'")) {
                    inQuote = false;
                }
                continue;
            }

            if (inCommentSlash) {
                if (aPart.startsWith("\n")) {
                    inCommentSlash = false;
                }
                continue;
            }

            if (inCommentAstrix) {
                if (aPart.startsWith("*/")) {
                    inCommentAstrix = false;
                    theString = theString.substring(1);
                }
                continue;
            }

            if (aPart.startsWith("'")) {
                inQuote = true;
                aString += aPart.charAt(0);
                continue;
            }

            if (aPart.startsWith("//")) {
                inCommentSlash = true;
                continue;
            }

            if (aPart.startsWith("/*")) {
                inCommentAstrix = true;
                continue;
            }

            aString += aPart.charAt(0);
        }

        return aString;
    }
}
