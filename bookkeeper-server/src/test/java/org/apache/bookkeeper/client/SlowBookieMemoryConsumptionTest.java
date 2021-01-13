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
package org.apache.bookkeeper.client;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufAllocator;
import io.netty.buffer.Unpooled;
import io.netty.util.internal.PlatformDependent;
import java.lang.management.ManagementFactory;
import java.lang.management.MemoryMXBean;
import lombok.extern.slf4j.Slf4j;
import static org.apache.bookkeeper.common.concurrent.FutureUtils.result;
import org.apache.bookkeeper.client.api.BookKeeper;
import org.apache.bookkeeper.client.api.DigestType;
import org.apache.bookkeeper.client.api.WriteHandle;
import org.apache.bookkeeper.net.BookieId;
import org.apache.bookkeeper.proto.BookieServer;
import org.apache.bookkeeper.test.BookKeeperClusterTestCase;
import org.junit.Test;

/**
 * Tests about memory consumption and slow bookies
 */
@Slf4j
public class SlowBookieMemoryConsumptionTest extends BookKeeperClusterTestCase {

    public SlowBookieMemoryConsumptionTest() {
        super(2);
    }

    @Test
    public void slowBookie() throws Exception {
        baseClientConf.setAddEntryTimeout(Integer.MAX_VALUE);
        try (BookKeeper bkc = BookKeeper.newBuilder(baseClientConf)
                .build();) {
            try (WriteHandle wh = result(bkc.newCreateLedgerOp()
                    .withAckQuorumSize(1)
                    .withWriteQuorumSize(2)
                    .withEnsembleSize(2)
                    .withPassword(new byte[0])
                    .withDigestType(DigestType.CRC32C)
                    .execute())) {
                BookieId oneBookie = wh.getLedgerMetadata().getEnsembleAt(0).get(0);
                BookieServer bookie = getBookieServer(oneBookie);
                bookie.suspendProcessing();
                MemoryMXBean mem = ManagementFactory.getMemoryMXBean();
                for (int i = 0; i < 100000; i++) {
                    ByteBuf payload = Unpooled.buffer(5 * 1024 * 1024);
                    payload.ensureWritable(5* 1024 * 1024);
                    wh.append(payload);
                    if (i % 100 == 0) {
                        log.info("sent {} mem {} dir {}", i, mem.getHeapMemoryUsage().getUsed(), PlatformDependent.usedDirectMemory());
                    }
                }
                bookie.resumeProcessing();
            }
        }
    }

}
