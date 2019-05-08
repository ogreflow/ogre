package com.ws.ogre.v2.commands.dataverifier;

public class VerificationUtils {
    public static String lcs(String theStr1, String theStr2) {
        int M = theStr1.length();
        int N = theStr2.length();
        StringBuffer aLCS = new StringBuffer();

        // opt[i][j] = length of LCS of theStr1[i..M] and theStr2[j..N]
        int[][] opt = new int[M + 1][N + 1];

        // Compute length of LCS (dynamic programming)
        for (int i = M - 1; i >= 0; i--) {
            for (int j = N - 1; j >= 0; j--) {
                if (theStr1.charAt(i) == theStr2.charAt(j))
                    opt[i][j] = opt[i + 1][j + 1] + 1;
                else
                    opt[i][j] = Math.max(opt[i + 1][j], opt[i][j + 1]);
            }
        }

        // Recover the LCS itself.
        int i = 0, j = 0;
        while (i < M && j < N) {
            if (theStr1.charAt(i) == theStr2.charAt(j)) {
                aLCS.append(theStr1.charAt(i));
                i++;
                j++;
            } else if (opt[i + 1][j] >= opt[i][j + 1]) i++;
            else j++;
        }

        return aLCS.toString();
    }

    public static boolean hasUnicodeChar(String theStr) {
        for (int i = 0; i < theStr.length(); i++) {
            if (theStr.charAt(i) > 128) {
                return true;
            }
        }

        return false;
    }
}
