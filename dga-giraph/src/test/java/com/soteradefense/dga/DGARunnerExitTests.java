package com.soteradefense.dga;


import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.security.Permission;

import static junit.framework.Assert.assertEquals;

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
