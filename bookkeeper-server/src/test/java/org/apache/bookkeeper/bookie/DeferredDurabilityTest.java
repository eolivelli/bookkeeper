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
package org.apache.bookkeeper.bookie;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import org.apache.bookkeeper.client.BKException.BKReadException;
import org.apache.bookkeeper.client.LedgerHandle;
import org.apache.bookkeeper.client.api.BKException;
import org.apache.bookkeeper.client.api.BookKeeper;
import org.apache.bookkeeper.client.api.LedgerEntry;
import org.apache.bookkeeper.client.api.LedgerType;
import org.apache.bookkeeper.client.api.ReadHandle;
import org.apache.bookkeeper.client.api.WriteAdvHandle;
import org.apache.bookkeeper.client.api.WriteHandle;
import static org.apache.bookkeeper.common.concurrent.FutureUtils.result;
import org.apache.bookkeeper.conf.ClientConfiguration;
import org.apache.bookkeeper.test.BookKeeperClusterTestCase;
import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


public class DeferredDurabilityTest extends BookKeeperClusterTestCase {

    private final static Logger LOG = LoggerFactory.getLogger(DeferredDurabilityTest.class);
    final ByteBuf data = Unpooled.wrappedBuffer("foobar".getBytes());

    public DeferredDurabilityTest() {
        super(1);
        baseConf.setJournalFlushWhenQueueEmpty(false);
    }

    @Test
    public void testAddEntry() throws Exception {
        int numEntries = 100;

        ClientConfiguration confWriter = new ClientConfiguration()
            .setZkServers(zkUtil.getZooKeeperConnectString());
        try (BookKeeper bkc = BookKeeper
            .newBuilder(confWriter)
            .build()) {
            long ledgerId;
            try (WriteHandle wh
                = result(bkc
                    .newCreateLedgerOp()
                    .withAckQuorumSize(1)
                    .withEnsembleSize(1)
                    .withWriteQuorumSize(1)
                    .withLedgerType(LedgerType.VD_JOURNAL)
                    .withPassword("testPasswd".getBytes())
                    .execute())) {
                LedgerHandle lh = (LedgerHandle) wh;
                ledgerId = wh.getId();
                for (int i = 0; i < numEntries - 1; i++) {
                    result(wh.append(data.copy()));
                }
                long lastEntryID = result(wh.append(data.copy()));
                assertEquals(numEntries - 1, lastEntryID);
                assertEquals(numEntries - 1, lh.getLastAddPushed());
                result(wh.sync());
            }
            try (ReadHandle rh = result(bkc.newOpenLedgerOp()
                .withLedgerId(ledgerId)
                .withPassword("testPasswd".getBytes())
                .execute())) {
                Iterable<LedgerEntry> entries = result(rh.read(0, numEntries - 1));
                checkEntries(entries, data.array());
            }
        }
    }

    @Test
    public void testPiggyBackLastAddSyncedEntryOnSync() throws Exception {
        int numEntries = 100;
        ClientConfiguration confWriter = new ClientConfiguration()
            .setZkServers(zkUtil.getZooKeeperConnectString());

        try (BookKeeper bkc = BookKeeper
            .newBuilder(confWriter)
            .build()) {
            long ledgerId;
            try (WriteAdvHandle wh
                = result(bkc
                    .newCreateLedgerOp()
                    .withAckQuorumSize(1)
                    .withEnsembleSize(1)
                    .withWriteQuorumSize(1)
                    .withLedgerType(LedgerType.VD_JOURNAL)
                    .withPassword("testPasswd".getBytes())
                    .makeAdv()
                    .execute())) {
                LedgerHandle lh = (LedgerHandle) wh;
                ledgerId = wh.getId();
                for (int i = 0; i < numEntries - 2; i++) {
                    long entryId = result(wh.write(i, data.copy()));
                    assertEquals(i, entryId);
                    // LAC should not advance on VD writes, it may advance because of grouping/flushQueueNotEmpty
                    // but in this test it should not advance till the given entry id
                    assertTrue(wh.getLastAddConfirmed() < entryId);
                }

                long entryId = result(wh.write(numEntries - 2, data.copy()));
                assertEquals(numEntries - 2, entryId);
                assertEquals(entryId, lh.getLastAddPushed());
                // forcing a sync, LAC will be able to advance
                long lastAddSynced = result(wh.sync());
                assertEquals(entryId, lastAddSynced);
                assertEquals(wh.getLastAddConfirmed(), lastAddSynced);
                long lastEntryId = result(wh.write(numEntries - 1, data.copy()));
                assertEquals(numEntries - 1, lastEntryId);
                assertEquals(lastEntryId, lh.getLastAddPushed());
                result(wh.sync());
                assertEquals(numEntries - 1, wh.getLastAddConfirmed());
                assertEquals(numEntries - 1, lh.getLastAddPushed());
            }
            try (ReadHandle rh = result(bkc.newOpenLedgerOp()
                .withLedgerId(ledgerId)
                .withPassword("testPasswd".getBytes())
                .execute())) {
                Iterable<LedgerEntry> entries = result(rh.read(0, numEntries - 1));
                checkEntries(entries, data.array());
            }
        }
    }

    @Test
    public void testNoSyncOnClose() throws Exception {
        int numEntries = 100;
        ClientConfiguration confWriter = new ClientConfiguration()
            .setZkServers(zkUtil.getZooKeeperConnectString());
        try (BookKeeper bkc = BookKeeper
            .newBuilder(confWriter)
            .build()) {
            long ledgerId;
            long lastAddConfirmedBeforeClose;
            try (WriteHandle wh
                = result(bkc
                    .newCreateLedgerOp()
                    .withAckQuorumSize(1)
                    .withEnsembleSize(1)
                    .withWriteQuorumSize(1)
                    .withLedgerType(LedgerType.VD_JOURNAL)
                    .withPassword("testPasswd".getBytes())
                    .execute())) {
                ledgerId = wh.getId();
                LedgerHandle lh = (LedgerHandle) wh;

                for (int i = 0; i < numEntries - 1; i++) {
                    result(wh.append(data.copy()));
                }
                long lastEntryId = result(wh.append(data.copy()));
                assertEquals(lastEntryId, lh.getLastAddPushed());

                // LastAddConfirmed on the client side will be < lh.getLastAddPushed()
                // as LastAddSynced is piggy backed on addResponse LastAddConfirmed may advance, depending on the
                // journal
                lastAddConfirmedBeforeClose = wh.getLastAddConfirmed();
                assertTrue(wh.getLastAddConfirmed() < lh.getLastAddPushed());

                // close operation does not automatically perform a 'sync' up to lastAddPushed
            }
            try (ReadHandle rh = result(bkc.newOpenLedgerOp()
                .withLedgerId(ledgerId)
                .withPassword("testPasswd".getBytes())
                .execute())) {
                if (lastAddConfirmedBeforeClose != -1)  {
                    checkEntries(result(rh.read(0, lastAddConfirmedBeforeClose)), data.array());
                }
                try {
                    result(rh.read(0, numEntries -1));
                    fail("should not be able to read up");
                } catch (BKReadException expected){
                }
            }
        }
    }

    @Test
    public void testSyncBeforeClose() throws Exception {
        int numEntries = 100;
        ClientConfiguration confWriter = new ClientConfiguration()
            .setZkServers(zkUtil.getZooKeeperConnectString());
        try (BookKeeper bkc = BookKeeper
            .newBuilder(confWriter)
            .build()) {
            long ledgerId;
            long lastAddConfirmedBeforeClose;
            try (WriteHandle wh
                = result(bkc
                    .newCreateLedgerOp()
                    .withAckQuorumSize(1)
                    .withEnsembleSize(1)
                    .withWriteQuorumSize(1)
                    .withLedgerType(LedgerType.VD_JOURNAL)
                    .withPassword("testPasswd".getBytes())
                    .execute())) {
                ledgerId = wh.getId();
                LedgerHandle lh = (LedgerHandle) wh;

                for (int i = 0; i < numEntries - 1; i++) {
                    result(wh.append(data.copy()));
                }
                long lastEntryId = result(wh.append(data.copy()));
                assertEquals(lastEntryId, lh.getLastAddPushed());

                // LastAddConfirmed on the client side will be < lh.getLastAddPushed()
                // as LastAddSynced is piggy backed on addResponse LastAddConfirmed may advance, depending on the
                // journal
                assertTrue(wh.getLastAddConfirmed() < lh.getLastAddPushed());

                result(wh.sync());
                lastAddConfirmedBeforeClose = wh.getLastAddConfirmed();
                assertEquals(numEntries - 1, lastAddConfirmedBeforeClose);
            }
            try (ReadHandle rh = result(bkc.newOpenLedgerOp()
                .withLedgerId(ledgerId)
                .withPassword("testPasswd".getBytes())
                .execute())) {
                checkEntries(result(rh.read(0, lastAddConfirmedBeforeClose)), data.array());
            }
        }
    }

    @Test
    public void testPiggyBackLastAddSyncedEntryOnWriteSyncLedger() throws Exception {
        int numEntries = 100;
        ClientConfiguration confWriter = new ClientConfiguration()
            .setZkServers(zkUtil.getZooKeeperConnectString());
        long lastEntryIdAfterSync;
        try (BookKeeper bkc = BookKeeper
            .newBuilder(confWriter)
            .build()) {
            long ledgerId;
            try (WriteHandle wh
                = result(bkc
                    .newCreateLedgerOp()
                    .withAckQuorumSize(1)
                    .withEnsembleSize(1)
                    .withWriteQuorumSize(1)
                    .withLedgerType(LedgerType.VD_JOURNAL)
                    .withPassword("testPasswd".getBytes())
                    .execute())) {
                LedgerHandle lh = (LedgerHandle) wh;
                ledgerId = wh.getId();
                for (int i = 0; i < numEntries - 2; i++) {
                    long entryId = result(wh.append(data.copy()));
                    assertEquals(i, entryId);
                    // LAC should not advance on VD writes, it may advance because of grouping/flushQueueNotEmpty
                    // but in this test it should not advance till the given entry id
                    assertTrue(wh.getLastAddConfirmed() < entryId);
                }

                long entryId = result(wh.append(data.copy()));
                assertEquals(numEntries - 2, entryId);
                assertEquals(entryId, lh.getLastAddPushed());
                // forcing a sync, LAC will be able to advance
                long lastAddSynced = result(wh.sync());
                assertEquals(entryId, lastAddSynced);
                assertEquals(wh.getLastAddConfirmed(), lastAddSynced);
                long lastEntryId = result(wh.append(data.copy()));
                assertEquals(numEntries - 1, lastEntryId);
                assertEquals(lastEntryId, lh.getLastAddPushed());

                forceSyncOnAllBookies(bkc);

                lastEntryIdAfterSync = result(wh.append(data.copy()));
                assertEquals(numEntries, lastEntryIdAfterSync);
                assertEquals(lastEntryIdAfterSync, lh.getLastAddPushed());

                // lastAddConfirmed surely MUST advance
                assertEquals(lastEntryIdAfterSync - 1, wh.getLastAddConfirmed());
            }
            try (ReadHandle rh = result(bkc.newOpenLedgerOp()
                .withLedgerId(ledgerId)
                .withPassword("testPasswd".getBytes())
                .execute())) {
                Iterable<LedgerEntry> entries = result(rh.read(0, lastEntryIdAfterSync - 1));
                checkEntries(entries, data.array());
            }
        }
    }

    void forceSyncOnAllBookies(final BookKeeper bkc1) throws Exception {
        // write a durable ledger, touching all of the bookies
        try (final WriteHandle wh2 = result(bkc1.newCreateLedgerOp()
            .withAckQuorumSize(numBookies)
            .withEnsembleSize(numBookies)
            .withWriteQuorumSize(numBookies)
            .withLedgerType(LedgerType.PD_JOURNAL)
            .withPassword("testPasswd".getBytes())
            .execute())) {
            result(wh2.append(data.copy()));
        }
    }

    @Test
    public void testRestartBookie() throws Exception {
        int numEntries = 100;
        ClientConfiguration confWriter = new ClientConfiguration()
            .setZkServers(zkUtil.getZooKeeperConnectString());
        long lastEntryIdAfterSync;
        try (BookKeeper bkc = BookKeeper
            .newBuilder(confWriter)
            .build()) {
            long ledgerId;
            long lastAddConfirmedBeforeSync;
            try (WriteHandle wh
                = result(bkc
                    .newCreateLedgerOp()
                    .withAckQuorumSize(1)
                    .withEnsembleSize(1)
                    .withWriteQuorumSize(1)
                    .withLedgerType(LedgerType.VD_JOURNAL)
                    .withPassword("testPasswd".getBytes())
                    .execute())) {
                LedgerHandle lh = (LedgerHandle) wh;
                ledgerId = wh.getId();
                for (int i = 0; i < numEntries - 1 ; i++) {
                    long entryId = result(wh.append(data.copy()));
                    assertEquals(i, entryId);
                    // LAC should not advance on VD writes, it may advance because of grouping/flushQueueNotEmpty
                    // but in this test it should not advance till the given entry id
                    assertTrue(wh.getLastAddConfirmed() < entryId);
                }

                lastAddConfirmedBeforeSync = wh.getLastAddConfirmed();
                forceSyncOnAllBookies(bkc);

                // restarting the bookie will probabily force the bookie to get the lastSyncedEntryId
                // from LedgerStorage
                restartBookies();

                lastEntryIdAfterSync = result(wh.append(data.copy()));
                assertEquals(numEntries - 1, lastEntryIdAfterSync);
                assertEquals(lastEntryIdAfterSync, lh.getLastAddPushed());

                // lastAddConfirmed surely MUST advance up to the last id written before the sync
                assertEquals(lastAddConfirmedBeforeSync, wh.getLastAddConfirmed());
            }
            try (ReadHandle rh = result(bkc.newOpenLedgerOp()
                .withLedgerId(ledgerId)
                .withPassword("testPasswd".getBytes())
                .execute())) {
                Iterable<LedgerEntry> entries = result(rh.read(0, lastAddConfirmedBeforeSync));
                checkEntries(entries, data.array());
            }
        }
    }

    private static void checkEntries(Iterable<LedgerEntry> entries, byte[] data)
        throws InterruptedException, BKException {
        for (org.apache.bookkeeper.client.api.LedgerEntry le : entries) {
            assertArrayEquals(data, le.getEntry());
        }
    }
}
