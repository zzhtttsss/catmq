package org.catmq.util;

import java.text.NumberFormat;

public class StringUtil {

    public static final String EMPTY_STRING = "";

    public static boolean isBlank(String str) {
        int strLen;
        if (str == null || (strLen = str.length()) == 0) {
            return true;
        }
        for (int i = 0; i < strLen; i++) {
            if (!Character.isWhitespace(str.charAt(i))) {
                return false;
            }
        }
        return true;
    }

    public static String concatString(CharSequence... strings) {

        StringBuilder sb = new StringBuilder();
        for (CharSequence str : strings) {
            sb.append(str);
        }
        return sb.toString();
    }


    public static String defaultString(final String str) {
        return defaultString(str, EMPTY_STRING);
    }

    public static String defaultString(final String str, final String defaultStr) {
        return str == null ? defaultStr : str;
    }

    public static String offset2FileName(final long offset) {
        final NumberFormat nf = NumberFormat.getInstance();
        nf.setMinimumIntegerDigits(20);
        nf.setMaximumFractionDigits(0);
        nf.setGroupingUsed(false);
        return nf.format(offset);
    }

}
