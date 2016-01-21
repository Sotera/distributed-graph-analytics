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

import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.yarn.conf.YarnConfiguration;

import java.security.PrivilegedAction;

public class DGAYarnRunner {
    public static void main(final String [] args) throws Exception {
        if (!UserGroupInformation.isSecurityEnabled()) {
            UserGroupInformation.createRemoteUser(YarnConfiguration.DEFAULT_NM_NONSECURE_MODE_LOCAL_USER).doAs(new PrivilegedAction<Void>() {
                @Override
                public Void run() {
                    try {
                        DGARunner runner = new DGARunner();
                        runner.run(args);
                    } catch (Exception e) {
                        e.printStackTrace();
                    }
                    return null;
                }
            });
        } else {
            DGARunner runner = new DGARunner();
            runner.run(args);
        }
    }
}
