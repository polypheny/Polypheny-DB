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

package org.polypheny.db.adapter.ethereum;

import java.io.IOException;
import java.math.BigInteger;
import java.util.List;
import java.util.function.Predicate;
import org.web3j.protocol.core.DefaultBlockParameter;
import org.web3j.protocol.core.methods.response.EthBlock;

public class TransactionReader extends BlockReader {

    private List<EthBlock.TransactionResult> transactionsList;
    private int transactionIndex;


    TransactionReader( String clientUrl, int blocks, Predicate<BigInteger> blockNumberPrecate ) {
        super( clientUrl, blocks, blockNumberPrecate );
        transactionIndex = -1;
    }


    @Override
    public String[] readNext() throws IOException {
        if ( this.blockReads <= 0 ) {
            return null;
        }

        while ( this.currentBlock.compareTo( BigInteger.ZERO ) == 1 && (transactionsList == null || transactionsList.size() == 0) ) {
            if ( blockNumberPredicate.test( this.currentBlock ) ) {
                transactionsList = web3j
                        .ethGetBlockByNumber( DefaultBlockParameter.valueOf( currentBlock ), true )
                        .send()
                        .getBlock()
                        .getTransactions();
                transactionIndex = 0;
                blockReads--;
            }
            this.currentBlock = this.currentBlock.subtract( BigInteger.ONE );
        }

        String[] res = EthereumMapper.TRANSACTION.map( transactionsList.get( transactionIndex++ ).get() );

        if ( transactionIndex >= transactionsList.size() ) {
            transactionsList = null;
        }

        return res;
    }

}
