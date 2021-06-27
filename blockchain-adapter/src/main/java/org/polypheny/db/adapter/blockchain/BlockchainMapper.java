/*
 * Copyright 2019-2021 The Polypheny Project
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

package org.polypheny.db.adapter.blockchain;

import org.web3j.protocol.core.methods.response.EthBlock;

import java.math.BigInteger;

public enum BlockchainMapper {
    BLOCK,
    TRANSACTION;

    static BlockchainMapper getMapper(String tableName) {
        if(tableName.equals("block")){
            return BLOCK;
        }
        return TRANSACTION;
    }
    public String[] map(Object obj){
        String[] str = null;

        if(this == BLOCK){

            EthBlock.Block blk = (EthBlock.Block) obj;

            str = new String[]{
                    new BigInteger(blk.getNumberRaw().substring(2),16).toString(),
                    blk.getHash(),
                    blk.getParentHash(),
                    blk.getNonceRaw(),
                    blk.getSha3Uncles(),
                    blk.getLogsBloom(),
                    blk.getTransactionsRoot(),
                    blk.getStateRoot(),
                    blk.getReceiptsRoot(),
                    blk.getAuthor(),
                    blk.getMiner(),
                    blk.getMixHash(),
                    blk.getDifficultyRaw(),
                    blk.getTotalDifficultyRaw(),
                    blk.getExtraData(),
                    blk.getSizeRaw(),
                    blk.getGasLimitRaw(),
                    blk.getGasUsedRaw(),
                    blk.getTimestampRaw(),
            };
        } else {
            EthBlock.TransactionObject tnx = (EthBlock.TransactionObject) obj;
            str = new String[]{
                    tnx.getHash(),
                    tnx.getNonceRaw(),
                    tnx.getBlockHash(),
                    tnx.getBlockNumberRaw(),
                    new BigInteger(tnx.getTransactionIndexRaw().substring(2),16).toString(),
                    tnx.getFrom(),
                    tnx.getTo(),
                    tnx.getValueRaw(),
                    tnx.getGasPriceRaw(),
                    tnx.getGasRaw(),
                    tnx.getInput(),
                    tnx.getCreates(),
                    tnx.getPublicKey(),
                    tnx.getRaw(),
                    tnx.getR(),
                    tnx.getS(),
            };
        }


        return str;
    }

    public BlockReader makeReader(String clientUrl) {
        if (this == BLOCK){
            return new BlockReader(clientUrl);
        }
        return new TransactionReader(clientUrl);
    }
}
