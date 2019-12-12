/*
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

package org.apache.bookkeeper.discover;

import static org.apache.bookkeeper.util.BookKeeperConstants.AVAILABLE_NODE;
import static org.apache.bookkeeper.util.BookKeeperConstants.COOKIE_NODE;
import static org.apache.bookkeeper.util.BookKeeperConstants.READONLY;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.Sets;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.Collections;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CopyOnWriteArraySet;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.function.BiConsumer;
import lombok.AccessLevel;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;
import org.apache.bookkeeper.client.BKException.Code;
import org.apache.bookkeeper.client.BKException.ZKException;
import org.apache.bookkeeper.common.concurrent.FutureUtils;
import org.apache.bookkeeper.common.util.SafeRunnable;
import org.apache.bookkeeper.net.BookieSocketAddress;
import org.apache.bookkeeper.versioning.LongVersion;
import org.apache.bookkeeper.versioning.Version;
import org.apache.bookkeeper.versioning.Version.Occurred;
import org.apache.bookkeeper.versioning.Versioned;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.Watcher.Event.EventType;
import org.apache.zookeeper.Watcher.Event.KeeperState;
import org.apache.zookeeper.ZooKeeper;
import org.apache.zookeeper.data.Stat;

/**
 * ZooKeeper based {@link RegistrationClient}.
 */
@Slf4j
public class ZKRegistrationClient implements RegistrationClient {

    private static final ObjectMapper MAPPER = new ObjectMapper();

    static final int ZK_CONNECT_BACKOFF_MS = 200;

    class WatchTask
        implements SafeRunnable,
                   Watcher,
                   BiConsumer<Versioned<Set<BookieSocketAddress>>, Throwable>,
                   AutoCloseable {

        private final String regPath;
        private final Set<RegistrationListener> listeners;
        private volatile boolean closed = false;
        private Set<BookieSocketAddress> bookies = null;
        private Version version = Version.NEW;
        private final CompletableFuture<Void> firstRunFuture;

        WatchTask(String regPath, CompletableFuture<Void> firstRunFuture) {
            this.regPath = regPath;
            this.listeners = new CopyOnWriteArraySet<>();
            this.firstRunFuture = firstRunFuture;
        }

        public int getNumListeners() {
            return listeners.size();
        }

        public boolean addListener(RegistrationListener listener) {
            if (listeners.add(listener)) {
                if (null != bookies) {
                    scheduler.execute(() -> {
                            listener.onBookiesChanged(
                                    new Versioned<>(bookies, version));
                        });
                }
            }
            return true;
        }

        public boolean removeListener(RegistrationListener listener) {
            return listeners.remove(listener);
        }

        void watch() {
            scheduleWatchTask(0L);
        }

        private void scheduleWatchTask(long delayMs) {
            try {
                scheduler.schedule(this, delayMs, TimeUnit.MILLISECONDS);
            } catch (RejectedExecutionException ree) {
                log.warn("Failed to schedule watch bookies task", ree);
            }
        }

        @Override
        public void safeRun() {
            if (isClosed()) {
                return;
            }

            getChildren(regPath, this)
                .whenCompleteAsync(this, scheduler);
        }

        @Override
        public void accept(Versioned<Set<BookieSocketAddress>> bookieSet, Throwable throwable) {
            if (throwable != null) {
                if (firstRunFuture.isDone()) {
                    scheduleWatchTask(ZK_CONNECT_BACKOFF_MS);
                } else {
                    firstRunFuture.completeExceptionally(throwable);
                }
                return;
            }

            if (this.version.compare(bookieSet.getVersion()) == Occurred.BEFORE) {
                this.version = bookieSet.getVersion();
                this.bookies = bookieSet.getValue();

                for (RegistrationListener listener : listeners) {
                    listener.onBookiesChanged(bookieSet);
                }
            }
            FutureUtils.complete(firstRunFuture, null);
        }

        @Override
        public void process(WatchedEvent event) {
            if (EventType.None == event.getType()) {
                if (KeeperState.Expired == event.getState()) {
                    scheduleWatchTask(ZK_CONNECT_BACKOFF_MS);
                }
                return;
            }

            // re-read the bookie list
            scheduleWatchTask(0L);
        }

        boolean isClosed() {
            return closed;
        }

        @Override
        public void close() {
            closed = true;
        }
    }

    private final ZooKeeper zk;
    private final ScheduledExecutorService scheduler;
    @Getter(AccessLevel.PACKAGE)
    private WatchTask watchWritableBookiesTask = null;
    @Getter(AccessLevel.PACKAGE)
    private WatchTask watchReadOnlyBookiesTask = null;

    // registration paths
    private final String bookieRegistrationPath;
    private final String bookieAllRegistrationPath;
    private final String bookieReadonlyRegistrationPath;

    public ZKRegistrationClient(ZooKeeper zk,
                                String ledgersRootPath,
                                ScheduledExecutorService scheduler) {
        this.zk = zk;
        this.scheduler = scheduler;

        this.bookieRegistrationPath = ledgersRootPath + "/" + AVAILABLE_NODE;
        this.bookieAllRegistrationPath = ledgersRootPath + "/" + COOKIE_NODE;
        this.bookieReadonlyRegistrationPath = this.bookieRegistrationPath + "/" + READONLY;
    }

    @Override
    public void close() {
        // no-op
    }

    public ZooKeeper getZk() {
        return zk;
    }

    @Override
    public CompletableFuture<Versioned<Set<BookieSocketAddress>>> getWritableBookies() {
        return getChildren(bookieRegistrationPath, null);
    }

    @Override
    public CompletableFuture<Versioned<Set<BookieSocketAddress>>> getAllBookies() {
        return getChildren(bookieAllRegistrationPath, null);
    }

    @Override
    public CompletableFuture<Versioned<Set<BookieSocketAddress>>> getReadOnlyBookies() {
        return getChildren(bookieReadonlyRegistrationPath, null);
    }

    @Override
    public CompletableFuture<Versioned<BookieServiceInfo>> getBookieServiceInfo(String bookieId) {
        String pathAsWritable = bookieRegistrationPath + "/" + bookieId;
        CompletableFuture<Versioned<BookieServiceInfo>> res = new CompletableFuture<>();
        zk.getData(pathAsWritable, false, (int rc, String path, Object o, byte[] bytes, Stat stat) -> {
            if (KeeperException.Code.OK.intValue() == rc) {
                BookieServiceInfo bookieServiceInfo = deserializeBookieService(bytes);
                res.complete(new Versioned<>(bookieServiceInfo, new LongVersion(stat.getCversion())));
            } else if (KeeperException.Code.NONODE.intValue() == rc) {
                // not found, looking for a readonly bookie
                String pathAsReadonly = bookieReadonlyRegistrationPath + "/" + bookieId;
                zk.getData(pathAsReadonly, false, (int rc2, String path2, Object o2, byte[] bytes2, Stat stat2) -> {
                    if (KeeperException.Code.OK.intValue() == rc2) {
                        BookieServiceInfo bookieServiceInfo = deserializeBookieService(bytes2);
                        res.complete(new Versioned<>(bookieServiceInfo, new LongVersion(stat2.getCversion())));
                    } else if (KeeperException.Code.NONODE.intValue() == rc2) {
                        // not found as readonly, the bookie is offline
                        // return an empty BookieServiceInfoStructure
                        res.complete(new Versioned<>(deserializeBookieService(null), new LongVersion(0)));
                    } else {
                        res.completeExceptionally(KeeperException.create(KeeperException.Code.get(rc2), path2));
                    }
                }, null);
            } else {
                res.completeExceptionally(KeeperException.create(KeeperException.Code.get(rc), path));
            }
        }, null);
        return res;
    }

    @SuppressWarnings("unchecked")
    private static BookieServiceInfo deserializeBookieService(byte[] bookieServiceInfo) {
        Map<String, String> map = Collections.emptyMap();
        if (bookieServiceInfo != null && bookieServiceInfo.length > 0) {
            try {
                map = Collections.unmodifiableMap(MAPPER.readValue(bookieServiceInfo, Map.class));
            } catch (IOException err) {
                log.error("Cannot deserialize bookieServiceInfo from "
                        + new String(bookieServiceInfo, StandardCharsets.US_ASCII), err);
            }
        }
        final Map<String, String> mapFinal = map;
        return new BookieServiceInfo() {
            @Override
            public Iterator<String> keys() {
                return mapFinal.keySet().iterator();
            }

            @Override
            public String get(String key, String defaultValue) {
                return mapFinal.getOrDefault(key, defaultValue);
            }
        };
    }


    private CompletableFuture<Versioned<Set<BookieSocketAddress>>> getChildren(String regPath, Watcher watcher) {
        CompletableFuture<Versioned<Set<BookieSocketAddress>>> future = FutureUtils.createFuture();
        zk.getChildren(regPath, watcher, (rc, path, ctx, children, stat) -> {
            if (Code.OK != rc) {
                ZKException zke = new ZKException();
                future.completeExceptionally(zke.fillInStackTrace());
                return;
            }

            Version version = new LongVersion(stat.getCversion());
            Set<BookieSocketAddress> bookies = convertToBookieAddresses(children);
            future.complete(new Versioned<>(bookies, version));
        }, null);
        return future;
    }


    @Override
    public synchronized CompletableFuture<Void> watchWritableBookies(RegistrationListener listener) {
        CompletableFuture<Void> f;
        if (null == watchWritableBookiesTask) {
            f = new CompletableFuture<>();
            watchWritableBookiesTask = new WatchTask(bookieRegistrationPath, f);
            f = f.whenComplete((value, cause) -> {
                if (null != cause) {
                    unwatchWritableBookies(listener);
                }
            });
        } else {
            f = watchWritableBookiesTask.firstRunFuture;
        }

        watchWritableBookiesTask.addListener(listener);
        if (watchWritableBookiesTask.getNumListeners() == 1) {
            watchWritableBookiesTask.watch();
        }
        return f;
    }

    @Override
    public synchronized void unwatchWritableBookies(RegistrationListener listener) {
        if (null == watchWritableBookiesTask) {
            return;
        }

        watchWritableBookiesTask.removeListener(listener);
        if (watchWritableBookiesTask.getNumListeners() == 0) {
            watchWritableBookiesTask.close();
            watchWritableBookiesTask = null;
        }
    }

    @Override
    public synchronized CompletableFuture<Void> watchReadOnlyBookies(RegistrationListener listener) {
        CompletableFuture<Void> f;
        if (null == watchReadOnlyBookiesTask) {
            f = new CompletableFuture<>();
            watchReadOnlyBookiesTask = new WatchTask(bookieReadonlyRegistrationPath, f);
            f = f.whenComplete((value, cause) -> {
                if (null != cause) {
                    unwatchReadOnlyBookies(listener);
                }
            });
        } else {
            f = watchReadOnlyBookiesTask.firstRunFuture;
        }

        watchReadOnlyBookiesTask.addListener(listener);
        if (watchReadOnlyBookiesTask.getNumListeners() == 1) {
            watchReadOnlyBookiesTask.watch();
        }
        return f;
    }

    @Override
    public synchronized void unwatchReadOnlyBookies(RegistrationListener listener) {
        if (null == watchReadOnlyBookiesTask) {
            return;
        }

        watchReadOnlyBookiesTask.removeListener(listener);
        if (watchReadOnlyBookiesTask.getNumListeners() == 0) {
            watchReadOnlyBookiesTask.close();
            watchReadOnlyBookiesTask = null;
        }
    }

    private static HashSet<BookieSocketAddress> convertToBookieAddresses(List<String> children) {
        // Read the bookie addresses into a set for efficient lookup
        HashSet<BookieSocketAddress> newBookieAddrs = Sets.newHashSet();
        for (String bookieAddrString : children) {
            if (READONLY.equals(bookieAddrString)) {
                continue;
            }

            BookieSocketAddress bookieAddr;
            try {
                bookieAddr = new BookieSocketAddress(bookieAddrString);
            } catch (IOException e) {
                log.error("Could not parse bookie address: " + bookieAddrString + ", ignoring this bookie");
                continue;
            }
            newBookieAddrs.add(bookieAddr);
        }
        return newBookieAddrs;
    }

}
