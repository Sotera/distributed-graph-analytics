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


import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.security.Permission;

import static org.junit.Assert.assertEquals;

public class DGARunnerExitTests {

    private SecurityManager defaultManager;
    protected static class ExitException extends SecurityException {
        public final int status;

        public ExitException(int status) {
            super("There is no escape!");
            this.status = status;
        }
    }

    private static class NoExitSecurityManager extends SecurityManager {
        @Override
        public void checkPermission(Permission perm) {
            // allow anything.
        }

        @Override
        public void checkPermission(Permission perm, Object context) {
            // allow anything.
        }

        @Override
        public void checkExit(int status) {
            super.checkExit(status);
            throw new ExitException(status);
        }
    }
    @Before
    public void setUp() throws Exception{
        defaultManager = System.getSecurityManager();
    }
    @Test
    public void testMalformedArguments() throws Exception {
        try {
            System.setSecurityManager(new NoExitSecurityManager());
            String[] args = {"wcc"};
            DGARunner.main(args);
        } catch (ExitException ex) {
            assertEquals(ex.status, 0);
        }
    }

    @Test
    public void testInvalidAnalytic() throws Exception {
        try {
            System.setSecurityManager(new NoExitSecurityManager());
            String[] args = {"dog", "/input/", "/output/"};
            DGARunner.main(args);
        } catch (ExitException ex) {
            assertEquals(ex.status, 0);
        }
    }

    @After
    public void cleanUp() throws Exception{
        System.setSecurityManager(defaultManager);
    }
}
