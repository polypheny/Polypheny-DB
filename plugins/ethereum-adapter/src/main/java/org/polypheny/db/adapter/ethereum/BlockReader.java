/*
 * Copyright 2019-2024 The Polypheny Project
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
 */

package org.polypheny.db.adapter.ethereum;


import java.io.Closeable;
import java.io.IOException;
import java.math.BigInteger;
import java.util.function.Predicate;
import org.polypheny.db.catalog.exceptions.GenericRuntimeException;
import org.web3j.protocol.Web3j;
import org.web3j.protocol.core.DefaultBlockParameter;
import org.web3j.protocol.core.methods.response.EthBlock;
import org.web3j.protocol.http.HttpService;

class BlockReader implements Closeable {

    protected final Web3j web3j;
    protected final Predicate<BigInteger> blockNumberPredicate;
    protected int blockReads;
    protected BigInteger currentBlock;


    BlockReader( String clientUrl, int blocks, Predicate<BigInteger> blockNumberPredicate ) {
        this.web3j = Web3j.build( new HttpService( clientUrl ) );
        this.blockReads = blocks;
        this.blockNumberPredicate = blockNumberPredicate;
        try {
            this.currentBlock = web3j.ethBlockNumber().send().getBlockNumber();
        } catch ( IOException e ) {
            throw new GenericRuntimeException( "Unable to connect to server: " + clientUrl );
        }
    }


    public String[] readNext() throws IOException {
        if ( this.blockReads <= 0 ) {
            return null;
        }
        EthBlock.Block block = null;
        while ( this.currentBlock.compareTo( BigInteger.ZERO ) == 1 && block == null ) {
            if ( blockNumberPredicate.test( this.currentBlock ) ) {
                block = web3j
                        .ethGetBlockByNumber( DefaultBlockParameter.valueOf( this.currentBlock ), false )
                        .send()
                        .getBlock();
                blockReads--;
            }
            this.currentBlock = this.currentBlock.subtract( BigInteger.ONE );
        }
        return block == null ? null : EthereumMapper.BLOCK.map( block );
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
