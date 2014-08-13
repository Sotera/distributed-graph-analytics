/*
 *
 *  Licensed to the Apache Software Foundation (ASF) under one
 *  or more contributor license agreements.  See the NOTICE file
 *  distributed with this work for additional information
 *  regarding copyright ownership.  The ASF licenses this file
 *  to you under the Apache License, Version 2.0 (the
 *  "License"); you may not use this file except in compliance
 *  with the License.  You may obtain a copy of the License at
 *       http://www.apache.org/licenses/LICENSE-2.0
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 *
 */

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
