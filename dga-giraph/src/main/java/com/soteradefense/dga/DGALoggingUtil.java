package com.soteradefense.dga;

import org.apache.hadoop.conf.Configuration;
import org.apache.log4j.ConsoleAppender;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.log4j.PatternLayout;

public class DGALoggingUtil {

    public static final String DGA_LOG_LEVEL = "dga.log.level";

    public static void setDGALogLevel(Configuration conf) {
        String logLevel = conf.get(DGA_LOG_LEVEL, "INFO");
        setDGALogLevel(logLevel);
    }

    public static void setDGALogLevel(String logLevel) {
        //System.out.println("Setting DGA Log level to: " + logLevel);
        Level level = Level.toLevel(logLevel, Level.INFO);
        ConsoleAppender console = new ConsoleAppender();
        //configure the appender
        String pattern = "%d [%p|%c] %m%n";
        console.setLayout(new PatternLayout(pattern));
        console.setThreshold(Level.DEBUG);
        console.activateOptions();
        //add appender to any Logger
        Logger.getLogger("com.soteradefense").addAppender(console);
        Logger.getLogger("com.soteradefense").setLevel(level);
    }
}
