/*
 * Copyright 2019-2020 The Polypheny Project
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *
 * This file incorporates code covered by the following terms:
 *
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to you under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.polypheny.db.adapter.blockchain;


import org.web3j.protocol.Web3j;
import org.web3j.protocol.core.DefaultBlockParameter;
import org.web3j.protocol.core.methods.response.EthBlock;
import org.web3j.protocol.http.HttpService;

import java.io.Closeable;
import java.io.IOException;
import java.math.BigInteger;

class BlockReader implements Closeable {

    protected final Web3j web3j;
    protected final BigInteger latestBlock;
    protected final BigInteger lastBlock;
    protected BigInteger currentBlock;

    /**
     * The default line to start reading.
     */
    public static final int DEFAULT_SKIP_LINES = 0;

    /**
     * The default file monitor delay.
     */
    public static final long DEFAULT_MONITOR_DELAY = 2000;


    BlockReader(String clientUrl, int blocks) {
        this.web3j = Web3j.build(new HttpService(clientUrl));
        try {
            this.latestBlock = web3j.ethBlockNumber().send().getBlockNumber();
            this.lastBlock = this.latestBlock.subtract(BigInteger.valueOf(blocks));
            this.currentBlock  = latestBlock;
        } catch (IOException e) {
            throw new RuntimeException("Unable to connect to server: "+clientUrl);
        }
    }



    public String[] readNext() throws IOException {
        if(this.currentBlock.compareTo(this.lastBlock) == -1)
            return null;
        EthBlock.Block block = web3j.ethGetBlockByNumber(DefaultBlockParameter.valueOf(this.currentBlock),false).send().getBlock();

        this.currentBlock = this.currentBlock.subtract(BigInteger.ONE);
        return BlockchainMapper.BLOCK.map(block);
    }


    /**
     * Closes the underlying reader.
     *
     * @throws IOException if the close fails
     */
    @Override
    public void close() throws IOException {
        this.web3j.shutdown();
    }
}

