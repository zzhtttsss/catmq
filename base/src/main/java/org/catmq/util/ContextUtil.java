package org.catmq.util;

import org.slf4j.MDC;

import java.util.Map;

public class ContextUtil {

    public static void restoreContext(Map<String, String> mdcContextMap) {
        if (mdcContextMap == null || mdcContextMap.isEmpty()) {
            MDC.clear();
        } else {
            MDC.setContextMap(mdcContextMap);
        }
    }
}
