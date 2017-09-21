/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with this
 * work for additional information regarding copyright ownership. The ASF
 * licenses this file to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */
package org.apache.bookkeeper.client.api;

import java.util.concurrent.CompletableFuture;
import org.apache.bookkeeper.client.BKException;

/**
 * Handle to manage an open ledger.
 *
 * @since 4.6
 */
public interface Handle extends AutoCloseable {

    /**
     * Get the id of the current ledger.
     *
     * @return the id of the ledger
     */
    long getId();

    /**
     * Close this ledger synchronously.
     *
     * @see #asyncClose
     */
    @Override
    void close() throws BKException, InterruptedException;

    /**
     * Asynchronous close, any adds in flight will return errors.
     *
     * <p>Closing a ledger will ensure that all clients agree on what the last
     * entry of the ledger is. This ensures that, once the ledger has been closed,
     * all reads from the ledger will return the same set of entries.
     *
     */
    CompletableFuture<?> asyncClose();

}
