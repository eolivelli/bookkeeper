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

import io.netty.buffer.ByteBuf;
import java.util.concurrent.CompletableFuture;

/**
 * Provide write access to a ledger
 *
 * @see WriteAdvHandler
 */
public interface WriteHandler extends ReadHandler {

    /**
     * Add entry asynchronously to an open ledger.
     *
     * @param data array of bytes to be written to the ledger
     * @return an handle to the result, in case of success it will return the id of the newly appended entry
     */
    public CompletableFuture<Long> append(byte[] data);

    /**
     * Add entry asynchronously to an open ledger.
     *
     * @param data array of bytes to be written to the ledger
     * @param offset offset from which to take bytes from data
     * @param length number of bytes to take from data
     * @return an handle to the result, in case of success it will return the id of the newly appended entry
     */
    public CompletableFuture<Long> append(byte[] data, int offset, int length);

    /**
     * Add entry asynchronously to an open ledger.
     *
     * @param data array of bytes to be written
     * @return an handle to the result, in case of success it will return the id of the newly appended entry
     */
    public CompletableFuture<Long> append(ByteBuf data);

}
