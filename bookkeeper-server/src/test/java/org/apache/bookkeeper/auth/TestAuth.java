/*
 *
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 *
 */
package org.apache.bookkeeper.auth;

import java.net.InetSocketAddress;
import java.util.Arrays;
import java.util.Enumeration;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

import org.apache.bookkeeper.client.BKException;
import org.apache.bookkeeper.client.BookKeeper;
import org.apache.bookkeeper.client.BookKeeper.DigestType;
import org.apache.bookkeeper.client.LedgerEntry;
import org.apache.bookkeeper.client.LedgerHandle;
import org.apache.bookkeeper.conf.ClientConfiguration;
import org.apache.bookkeeper.conf.ServerConfiguration;
import org.apache.bookkeeper.proto.BookieServer;
import org.apache.bookkeeper.proto.BookkeeperInternalCallbacks.GenericCallback;
import org.apache.bookkeeper.test.BookKeeperClusterTestCase;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static org.junit.Assert.*;

import org.junit.Test;


public class TestAuth extends BookKeeperClusterTestCase {
    static final Logger LOG = LoggerFactory.getLogger(TestAuth.class);
    public static final String TEST_AUTH_PROVIDER_PLUGIN_NAME = "TestAuthProviderPlugin";
    private static final byte[] PASSWD = "testPasswd".getBytes();
    private static final byte[] ENTRY = "TestEntry".getBytes();

    private static final byte[] SUCCESS_RESPONSE = {1};
    private static final byte[] FAILURE_RESPONSE = {2};
    private static final byte[] PAYLOAD_MESSAGE = {3};

    public TestAuth() {
        super(0); // start them later when auth providers are configured
    }

    // we pass in ledgerId because the method may throw exceptions
    private void connectAndWriteToBookie(ClientConfiguration conf, AtomicLong ledgerWritten)
            throws Exception {
        LOG.info("Connecting to bookie");
        BookKeeper bkc = new BookKeeper(conf, zkc);
        LedgerHandle l = bkc.createLedger(1, 1, DigestType.CRC32,
                PASSWD);
        ledgerWritten.set(l.getId());
        l.addEntry(ENTRY);
        l.close();
        bkc.close();
    }

    /**
     * check if the entry exists. Restart the bookie to allow
     * access
     */
    private int entryCount(long ledgerId, ServerConfiguration bookieConf,
                           ClientConfiguration clientConf) throws Exception {
        LOG.info("Counting entries in {}", ledgerId);
        for (ServerConfiguration conf : bsConfs) {
            conf.setBookieAuthProviderFactoryClass(
                    AlwaysSucceedBookieAuthProviderFactory.class.getName());
        }
        clientConf.setClientAuthProviderFactoryClass(
                SendUntilCompleteClientAuthProviderFactory.class.getName());
        
        restartBookies();

        BookKeeper bkc = new BookKeeper(clientConf, zkc);
        LedgerHandle lh = bkc.openLedger(ledgerId, DigestType.CRC32,
                                         PASSWD);
        if (lh.getLastAddConfirmed() < 0) {
            return 0;
        }
        Enumeration<LedgerEntry> e = lh.readEntries(0, lh.getLastAddConfirmed());
        int count = 0;
        while (e.hasMoreElements()) {
            count++;
            assertTrue("Should match what we wrote",
                       Arrays.equals(e.nextElement().getEntry(), ENTRY));
        }
        return count;
    }

    /**
     * Test an connection will authorize with a single message
     * to the server and a single response.
     */
    @Test(timeout=30000)
    public void testSingleMessageAuth() throws Exception {
        ServerConfiguration bookieConf = newServerConfiguration();
        bookieConf.setBookieAuthProviderFactoryClass(
                AlwaysSucceedBookieAuthProviderFactory.class.getName());
        
        ClientConfiguration clientConf = newClientConfiguration();
        clientConf.setClientAuthProviderFactoryClass(
                SendUntilCompleteClientAuthProviderFactory.class.getName());
        
        startAndStoreBookie(bookieConf);

        AtomicLong ledgerId = new AtomicLong(-1);
        connectAndWriteToBookie(clientConf, ledgerId); // should succeed

        assertFalse(ledgerId.get() == -1);
        assertEquals("Should have entry", 1, entryCount(ledgerId.get(), bookieConf, clientConf));
    }
    
    /**
     * Test that when the bookie provider sends a failure message
     * the client will not be able to write
     */
    @Test(timeout=30000)
    public void testSingleMessageAuthFailure() throws Exception {
        ServerConfiguration bookieConf = newServerConfiguration();
        bookieConf.setBookieAuthProviderFactoryClass(
                AlwaysFailBookieAuthProviderFactory.class.getName());
        
        ClientConfiguration clientConf = newClientConfiguration();
        clientConf.setClientAuthProviderFactoryClass(
                SendUntilCompleteClientAuthProviderFactory.class.getName());
        
        startAndStoreBookie(bookieConf);

        AtomicLong ledgerId = new AtomicLong(-1);
        try {
            connectAndWriteToBookie(clientConf, ledgerId); // should fail
            fail("Shouldn't get this far");
        } catch (BKException.BKUnauthorizedAccessException bke) {
            // client shouldnt be able to find enough bookies to
            // write
        }
        assertFalse(ledgerId.get() == -1);
        assertEquals("Shouldn't have entry", 0, entryCount(ledgerId.get(), bookieConf, clientConf));
    }

    /**
     * Test that authentication works when the providers
     * exchange multiple messages
     */
    @Test(timeout=30000)
    public void testMultiMessageAuth() throws Exception {
        ServerConfiguration bookieConf = newServerConfiguration();
        bookieConf.setBookieAuthProviderFactoryClass(
                SucceedAfter3BookieAuthProviderFactory.class.getName());
        
        ClientConfiguration clientConf = newClientConfiguration();
        clientConf.setClientAuthProviderFactoryClass(
                SendUntilCompleteClientAuthProviderFactory.class.getName());
        
        AtomicLong ledgerId = new AtomicLong(-1);
        startAndStoreBookie(bookieConf);
        connectAndWriteToBookie(clientConf, ledgerId); // should succeed

        assertFalse(ledgerId.get() == -1);
        assertEquals("Should have entry", 1, entryCount(ledgerId.get(), bookieConf, clientConf));
    }
    
    /**
     * Test that when the bookie provider sends a failure message
     * the client will not be able to write
     */
    @Test(timeout=30000)
    public void testMultiMessageAuthFailure() throws Exception {
        ServerConfiguration bookieConf = newServerConfiguration();
        bookieConf.setBookieAuthProviderFactoryClass(
                FailAfter3BookieAuthProviderFactory.class.getName());
        
        ClientConfiguration clientConf = newClientConfiguration();
        clientConf.setClientAuthProviderFactoryClass(
                SendUntilCompleteClientAuthProviderFactory.class.getName());
        
        startAndStoreBookie(bookieConf);

        AtomicLong ledgerId = new AtomicLong(-1);
        try {
            connectAndWriteToBookie(clientConf, ledgerId); // should fail
            fail("Shouldn't get this far");
        } catch (BKException.BKUnauthorizedAccessException bke) {
            // bookie should have sent a negative response before
            // breaking the conneciton
        }
        assertFalse(ledgerId.get() == -1);
        assertEquals("Shouldn't have entry", 0, entryCount(ledgerId.get(), bookieConf, clientConf));
    }

    /**
     * Test that when the bookie and the client have a different
     * plugin configured, no messages will get through.
     */
    @Test(timeout=30000)
    public void testDifferentPluginFailure() throws Exception {
        ServerConfiguration bookieConf = newServerConfiguration();
        bookieConf.setBookieAuthProviderFactoryClass(
                DifferentPluginBookieAuthProviderFactory.class.getName());
        
        ClientConfiguration clientConf = newClientConfiguration();
        clientConf.setClientAuthProviderFactoryClass(
                SendUntilCompleteClientAuthProviderFactory.class.getName());
        
        startAndStoreBookie(bookieConf);
        AtomicLong ledgerId = new AtomicLong(-1);
        try {
            connectAndWriteToBookie(clientConf, ledgerId); // should fail
            fail("Shouldn't get this far");
        } catch (BKException.BKUnauthorizedAccessException bke) {
            // bookie should have sent a negative response before
            // breaking the conneciton
        }
        assertFalse(ledgerId.get() == -1);
        assertEquals("Shouldn't have entry", 0, entryCount(ledgerId.get(), bookieConf, clientConf));
    }

    /**
     * Test that when the plugin class does exist, but
     * doesn't implement the interface, we fail predictably
     */
    @Test(timeout=30000)
    public void testExistantButNotValidPlugin() throws Exception {
        ServerConfiguration bookieConf = newServerConfiguration();
        bookieConf.setBookieAuthProviderFactoryClass(
                "java.lang.String");

        ClientConfiguration clientConf = newClientConfiguration();
        clientConf.setClientAuthProviderFactoryClass(
                "java.lang.String");
        try {
            startAndStoreBookie(bookieConf);
            fail("Shouldn't get this far");
        } catch (RuntimeException e) {
            // received correct exception
            assertTrue("Wrong exception thrown",
                    e.getMessage().contains("not "
                            + BookieAuthProvider.Factory.class.getName()));
        }

        try {
            BookKeeper bkc = new BookKeeper(clientConf, zkc);
            fail("Shouldn't get this far");
        } catch (RuntimeException e) {
            // received correct exception
            assertTrue("Wrong exception thrown",
                    e.getMessage().contains("not "
                            + ClientAuthProvider.Factory.class.getName()));
        }
    }

    /**
     * Test that when the plugin class does not exist,
     * the bookie will not start and the client will
     * break.
     */
    @Test(timeout=30000)
    public void testNonExistantPlugin() throws Exception {
        ServerConfiguration bookieConf = newServerConfiguration();
        bookieConf.setBookieAuthProviderFactoryClass(
                "NonExistantClassNameForTestingAuthPlugins");
        
        ClientConfiguration clientConf = newClientConfiguration();
        clientConf.setClientAuthProviderFactoryClass(
                "NonExistantClassNameForTestingAuthPlugins");
        try {
            startAndStoreBookie(bookieConf);
            fail("Shouldn't get this far");
        } catch (RuntimeException e) {
            // received correct exception
            assertEquals("Wrong exception thrown",
                    e.getCause().getClass(), ClassNotFoundException.class);
        }

        try {
            BookKeeper bkc = new BookKeeper(clientConf, zkc);
            fail("Shouldn't get this far");
        } catch (RuntimeException e) {
            // received correct exception
            assertEquals("Wrong exception thrown",
                    e.getCause().getClass(), ClassNotFoundException.class);
        }
    }

    /**
     * Test that when the plugin on the bookie crashes, the client doesn't
     * hang also, but it cannot write in any case.
     */
    @Test(timeout=30000)
    public void testCrashDuringAuth() throws Exception {
        ServerConfiguration bookieConf = newServerConfiguration();
        bookieConf.setBookieAuthProviderFactoryClass(
                CrashAfter3BookieAuthProviderFactory.class.getName());
        
        ClientConfiguration clientConf = newClientConfiguration();
        clientConf.setClientAuthProviderFactoryClass(
                SendUntilCompleteClientAuthProviderFactory.class.getName());

        startAndStoreBookie(bookieConf);

        AtomicLong ledgerId = new AtomicLong(-1);
        try {
            connectAndWriteToBookie(clientConf, ledgerId);
            fail("Shouldn't get this far");
        } catch (BKException.BKNotEnoughBookiesException bke) {
            // bookie won't respond, request will timeout, and then
            // we wont be able to find a replacement
        }
        assertFalse(ledgerId.get() == -1);
        assertEquals("Shouldn't have entry", 0, entryCount(ledgerId.get(), bookieConf, clientConf));
    }

    /**
     * Test that when a bookie simply stops replying during auth, the client doesn't
     * hang also, but it cannot write in any case.
     */
    @Test(timeout=30000)
    public void testCrashType2DuringAuth() throws Exception {
        ServerConfiguration bookieConf = newServerConfiguration();
        bookieConf.setBookieAuthProviderFactoryClass(
                CrashType2After3BookieAuthProviderFactory.class.getName());
        
        ClientConfiguration clientConf = newClientConfiguration();
        clientConf.setClientAuthProviderFactoryClass(
                SendUntilCompleteClientAuthProviderFactory.class.getName());
        crashType2bookieInstance = startAndStoreBookie(bookieConf);

        AtomicLong ledgerId = new AtomicLong(-1);
        try {
            connectAndWriteToBookie(clientConf, ledgerId);
            fail("Shouldn't get this far");
        } catch (BKException.BKNotEnoughBookiesException bke) {
            // bookie won't respond, request will timeout, and then
            // we wont be able to find a replacement
        }
        assertFalse(ledgerId.get() == -1);
        assertEquals("Shouldn't have entry", 0, entryCount(ledgerId.get(), bookieConf, clientConf));
    }

    BookieServer startAndStoreBookie(ServerConfiguration conf) throws Exception {
        bsConfs.add(conf);
        BookieServer s = startBookie(conf);
        bs.add(s);
        return s;
    }

    public static class AlwaysSucceedBookieAuthProviderFactory
        implements BookieAuthProvider.Factory {
        @Override
        public String getPluginName() {
            return TEST_AUTH_PROVIDER_PLUGIN_NAME;
        }

        @Override
        public void init(ServerConfiguration conf) {
        }

        @Override
        public BookieAuthProvider newProvider(InetSocketAddress addr,
                                              final GenericCallback<Void> completeCb) {
            return new BookieAuthProvider() {
                public void process(AuthMessageData m, GenericCallback<AuthMessageData> cb) {
                    cb.operationComplete(BKException.Code.OK, AuthMessageData.wrap(SUCCESS_RESPONSE));
                    completeCb.operationComplete(BKException.Code.OK, null);
                }
            };
        }
    }

    public static class AlwaysFailBookieAuthProviderFactory
        implements BookieAuthProvider.Factory {
        @Override
        public String getPluginName() {
            return TEST_AUTH_PROVIDER_PLUGIN_NAME;
        }

        @Override
        public void init(ServerConfiguration conf) {
        }

        @Override
        public BookieAuthProvider newProvider(InetSocketAddress addr,
                                              final GenericCallback<Void> completeCb) {
            return new BookieAuthProvider() {
                public void process(AuthMessageData m, GenericCallback<AuthMessageData> cb) {
                    cb.operationComplete(BKException.Code.OK, AuthMessageData.wrap(FAILURE_RESPONSE));
                    completeCb.operationComplete(
                            BKException.Code.UnauthorizedAccessException, null);
                }
            };
        }
    }

    private static class SendUntilCompleteClientAuthProviderFactory
        implements ClientAuthProvider.Factory {

        @Override
        public String getPluginName() {
            return TEST_AUTH_PROVIDER_PLUGIN_NAME;
        }

        @Override
        public void init(ClientConfiguration conf) {
        }

        @Override
        public ClientAuthProvider newProvider(InetSocketAddress addr,
                final GenericCallback<Void> completeCb) {
            return new ClientAuthProvider() {
                public void init(GenericCallback<AuthMessageData> cb) {
                    cb.operationComplete(BKException.Code.OK, AuthMessageData.wrap(PAYLOAD_MESSAGE));
                }
                public void process(AuthMessageData m, GenericCallback<AuthMessageData> cb) {
                    byte[] type = m.getData();
                    if (Arrays.equals(type,SUCCESS_RESPONSE)) {
                        completeCb.operationComplete(BKException.Code.OK, null);
                    } else if (Arrays.equals(type,FAILURE_RESPONSE)) {
                        completeCb.operationComplete(BKException.Code.UnauthorizedAccessException, null);
                    } else {
                        cb.operationComplete(BKException.Code.OK, AuthMessageData.wrap(PAYLOAD_MESSAGE));
                    }
                }
            };
        }
    }

    public static class SucceedAfter3BookieAuthProviderFactory
        implements BookieAuthProvider.Factory {
        AtomicInteger numMessages = new AtomicInteger(0);

        @Override
        public String getPluginName() {
            return TEST_AUTH_PROVIDER_PLUGIN_NAME;
        }

        @Override
        public void init(ServerConfiguration conf) {
        }

        @Override
        public BookieAuthProvider newProvider(InetSocketAddress addr,
                                              final GenericCallback<Void> completeCb) {
            return new BookieAuthProvider() {
                public void process(AuthMessageData m, GenericCallback<AuthMessageData> cb) {
                    if (numMessages.incrementAndGet() == 3) {
                        cb.operationComplete(BKException.Code.OK, AuthMessageData.wrap(SUCCESS_RESPONSE));
                        completeCb.operationComplete(BKException.Code.OK, null);
                    } else {
                        cb.operationComplete(BKException.Code.OK, AuthMessageData.wrap(PAYLOAD_MESSAGE));
                    }
                }
            };
        }
    }

    public static class FailAfter3BookieAuthProviderFactory
        implements BookieAuthProvider.Factory {
        AtomicInteger numMessages = new AtomicInteger(0);

        @Override
        public String getPluginName() {
            return TEST_AUTH_PROVIDER_PLUGIN_NAME;
        }

        @Override
        public void init(ServerConfiguration conf) {
        }

        @Override
        public BookieAuthProvider newProvider(InetSocketAddress addr,
                                              final GenericCallback<Void> completeCb) {
            return new BookieAuthProvider() {
                public void process(AuthMessageData m, GenericCallback<AuthMessageData> cb) {
                    if (numMessages.incrementAndGet() == 3) {
                        cb.operationComplete(BKException.Code.OK, AuthMessageData.wrap(FAILURE_RESPONSE));
                        completeCb.operationComplete(BKException.Code.UnauthorizedAccessException,
                                                     null);
                    } else {
                        cb.operationComplete(BKException.Code.OK, AuthMessageData.wrap(PAYLOAD_MESSAGE));
                    }
                }
            };
        }
    }

    public static class CrashAfter3BookieAuthProviderFactory
        implements BookieAuthProvider.Factory {
        AtomicInteger numMessages = new AtomicInteger(0);

        @Override
        public String getPluginName() {
            return TEST_AUTH_PROVIDER_PLUGIN_NAME;
        }

        @Override
        public void init(ServerConfiguration conf) {
        }

        @Override
        public BookieAuthProvider newProvider(InetSocketAddress addr,
                                              final GenericCallback<Void> completeCb) {
            return new BookieAuthProvider() {
                public void process(AuthMessageData m, GenericCallback<AuthMessageData> cb) {
                    if (numMessages.incrementAndGet() == 3) {
                        throw new RuntimeException("Do bad things to the bookie");
                    } else {
                        cb.operationComplete(BKException.Code.OK, AuthMessageData.wrap(PAYLOAD_MESSAGE));
                    }
                }
            };
        }
    }

    private static BookieServer crashType2bookieInstance = null;
    public static class CrashType2After3BookieAuthProviderFactory
        implements BookieAuthProvider.Factory {
        AtomicInteger numMessages = new AtomicInteger(0);

        @Override
        public String getPluginName() {
            return TEST_AUTH_PROVIDER_PLUGIN_NAME;
        }

        @Override
        public void init(ServerConfiguration conf) {
        }

        @Override
        public BookieAuthProvider newProvider(InetSocketAddress addr,
                                              final GenericCallback<Void> completeCb) {
            return new BookieAuthProvider() {
                public void process(AuthMessageData m, GenericCallback<AuthMessageData> cb) {
                    if (numMessages.incrementAndGet() != 3) {
                        cb.operationComplete(BKException.Code.OK, AuthMessageData.wrap(PAYLOAD_MESSAGE));
                        return;
                    }
                    crashType2bookieInstance.suspendProcessing();
                }
            };
        }
    }

    public static class DifferentPluginBookieAuthProviderFactory
        implements BookieAuthProvider.Factory {
        @Override
        public String getPluginName() {
            return "DifferentAuthProviderPlugin";
        }

        @Override
        public void init(ServerConfiguration conf) {
        }

        @Override
        public BookieAuthProvider newProvider(InetSocketAddress addr,
                                              final GenericCallback<Void> completeCb) {
            return new BookieAuthProvider() {
                public void process(AuthMessageData m, GenericCallback<AuthMessageData> cb) {
                    cb.operationComplete(BKException.Code.OK, AuthMessageData.wrap(FAILURE_RESPONSE));
                    completeCb.operationComplete(BKException.Code.OK, null);
                }
            };
        }
    }

}
