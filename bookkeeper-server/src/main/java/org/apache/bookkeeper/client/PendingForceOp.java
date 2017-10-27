/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.bookkeeper.client;

import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import org.apache.bookkeeper.proto.BookkeeperInternalCallbacks;
import org.apache.bookkeeper.net.BookieSocketAddress;
import org.apache.bookkeeper.stats.OpStatsLogger;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * This represents a pending Sync operation. When it has got
 * success from Ack Quorum bookies, sends success back to the application,
 * otherwise failure is sent back to the caller.
 *
 */
class PendingForceOp implements BookkeeperInternalCallbacks.ForceCallback {
    private final static Logger LOG = LoggerFactory.getLogger(PendingForceOp.class);
    final CompletableFuture<Long> cb;
    final Set<Integer> writeSet;
    final Set<Integer> receivedResponseSet;

    final DistributionSchedule.AckSet ackSet;
    boolean completed = false;
    int lastSeenError = BKException.Code.WriteException;

    final LedgerHandle lh;
    final OpStatsLogger forceOpLogger;

    PendingForceOp(LedgerHandle lh, CompletableFuture<Long> cb) {
        this.lh = lh;
        this.cb = cb;
        ackSet = lh.distributionSchedule.getAckSet();
        forceOpLogger = lh.bk.getForceOpLogger();
        this.writeSet = new HashSet<>(lh.distributionSchedule.getWriteSet(0));
        this.receivedResponseSet = new HashSet<>(writeSet);
    }

    void sendSyncRequest(int bookieIndex) {
        lh.bk.getBookieClient().force(lh.metadata.currentEnsemble.get(bookieIndex), lh.ledgerId,
                                     lh.ledgerKey, this, bookieIndex);
    }

    void initiate() {
        for (int bookieIndex: writeSet) {
            sendSyncRequest(bookieIndex);
        }
    }

    @Override
    public void forceComplete(int rc, long ledgerId, long lastSyncedEntryId, BookieSocketAddress addr, Object ctx) {
        int bookieIndex = (Integer) ctx;
        if (LOG.isDebugEnabled()) {
            LOG.debug("forceComplete {} {} {} {}", rc, ledgerId, lastSyncedEntryId, addr);
        }

        if (completed) {
            return;
        }

        if (BKException.Code.OK != rc) {
            lastSeenError = rc;
        }

        // We got response.
        receivedResponseSet.remove(bookieIndex);

        if (rc == BKException.Code.OK) {
            if (ackSet.completeBookieAndCheck(bookieIndex) && !completed) {
                lh.lastAddSyncedManager.updateBookie(bookieIndex, lastSyncedEntryId);
                completed = true;
                long actualLastAddConfirmed = lh.forceCompleted();
                cb.complete(actualLastAddConfirmed);
                return;
            }
        } else {
            LOG.warn("Sync did not succeed: Ledger {} on {} code {}", new Object[] { ledgerId, addr, rc});
        }

        if (receivedResponseSet.isEmpty()){
            completed = true;
            cb.completeExceptionally(BKException.create(lastSeenError));
        }
    }
}