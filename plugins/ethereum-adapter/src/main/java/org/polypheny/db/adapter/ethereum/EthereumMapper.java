/*
 * Copyright 2019-2023 The Polypheny Project
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

import java.math.BigInteger;
import java.util.function.Predicate;
import org.web3j.protocol.core.methods.response.EthBlock;

public enum EthereumMapper {

    BLOCK,
    TRANSACTION;


    static EthereumMapper getMapper( String tableName ) {
        if ( tableName.equals( "block" ) ) {
            return BLOCK;
        }
        return TRANSACTION;
    }


    static String safeToString( Object x ) {
        if ( x == null ) {
            return null;
        }
        return x.toString();
    }


    public String[] map( Object obj ) {
        String[] str = null;

        if ( this == BLOCK ) {

            EthBlock.Block blk = (EthBlock.Block) obj;
            str = new String[]{
                    safeToString( blk.getNumber() ),
                    blk.getHash(),
                    blk.getParentHash(),
                    safeToString( blk.getNonce() ),
                    blk.getSha3Uncles(),
                    blk.getLogsBloom(),
                    blk.getTransactionsRoot(),
                    blk.getStateRoot(),
                    blk.getReceiptsRoot(),
                    blk.getAuthor(),
                    blk.getMiner(),
                    blk.getMixHash(),
                    safeToString( blk.getDifficulty() ),
                    safeToString( blk.getTotalDifficulty() ),
                    blk.getExtraData(),
                    safeToString( blk.getSize() ),
                    safeToString( blk.getGasLimit() ),
                    safeToString( blk.getGasUsed() ),
                    safeToString( blk.getTimestamp() ),
            };
        } else {
            EthBlock.TransactionObject tnx = (EthBlock.TransactionObject) obj;
            str = new String[]{
                    tnx.getHash(),
                    safeToString( tnx.getNonce() ),
                    tnx.getBlockHash(),
                    safeToString( tnx.getBlockNumber() ),
                    safeToString( tnx.getTransactionIndex() ),
                    tnx.getFrom(),
                    tnx.getTo(),
                    safeToString( tnx.getValue() ),
                    safeToString( tnx.getGasPrice() ),
                    safeToString( tnx.getGas() ),
                    tnx.getInput(),
                    tnx.getCreates(),
                    tnx.getPublicKey(),
                    tnx.getRaw(),
                    tnx.getR(),
                    tnx.getS()
            };
        }

        return str;
    }


    public BlockReader makeReader( String clientUrl, int blocks, Predicate<BigInteger> blockNumberPredicate ) {
        if ( this == BLOCK ) {
            return new BlockReader( clientUrl, blocks, blockNumberPredicate );
        }
        return new TransactionReader( clientUrl, blocks, blockNumberPredicate );
    }
}
