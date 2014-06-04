package com.soteradefense.dga;

import org.apache.hadoop.conf.Configuration;
import org.apache.log4j.Level;
import org.slf4j.Logger;

public class DGALoggingUtil {

    public static final String IS_DEBUG_ENABLED = "dga.debug";

    public static void updateLoggerBasedOnConfiguration(Configuration conf, Logger logger) {
        boolean isDebugEnabled = false;
        try {
            isDebugEnabled = Boolean.parseBoolean(conf.get(IS_DEBUG_ENABLED));
        } catch (Exception e) {
            // do nothing
        }
        if (isDebugEnabled) {
            if (logger instanceof org.apache.log4j.Logger) {
                org.apache.log4j.Logger implLogger = (org.apache.log4j.Logger) logger;
                implLogger.setLevel(Level.DEBUG);
            }
        }
    }
}
