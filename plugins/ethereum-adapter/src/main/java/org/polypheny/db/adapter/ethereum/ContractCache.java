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
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.stream.Collectors;
import lombok.extern.slf4j.Slf4j;
import org.polypheny.db.adapter.DataSource.ExportedColumn;
import org.polypheny.db.adapter.ethereum.CachingStatus.ProcessingState;
import org.polypheny.db.ddl.DdlManager.FieldInformation;
import org.web3j.protocol.Web3j;
import org.web3j.protocol.http.HttpService;

@Slf4j
public class ContractCache {

    public final int sourceAdapterId;
    private final int targetAdapterId;
    private final Map<String, List<ExportedColumn>> columns;
    private final int batchSizeInBlocks;
    private final BigInteger fromBlock;
    private final BigInteger toBlock;
    private BigInteger currentBlock;
    private boolean hasError = false;
    private String errorMessage;


    private final Map<String, EventCache> cache = new ConcurrentHashMap<>(); // a cache for each event
    private final Map<String, List<EventData>> eventsPerContract;
    protected final Web3j web3j;


    public ContractCache( int sourceAdapterId, int targetAdapterId, String clientUrl, int batchSizeInBlocks, BigInteger fromBlock, BigInteger toBlock, Map<String, List<EventData>> eventsPerContract, Map<String, List<ExportedColumn>> columns ) {
        this.sourceAdapterId = sourceAdapterId;
        this.targetAdapterId = targetAdapterId;
        this.columns = columns;
        this.batchSizeInBlocks = batchSizeInBlocks;
        this.fromBlock = fromBlock;
        this.currentBlock = fromBlock;
        this.toBlock = toBlock;
        this.eventsPerContract = eventsPerContract;
        this.web3j = Web3j.build( new HttpService( clientUrl ) );
        eventsPerContract.forEach( ( address, events ) -> this.cache.put( address, new EventCache( events, web3j ) ) );
    }


    public void initializeCaching() {
        // register table in schema
        this.createSchema();
        // start caching
        this.startCaching();
    }


    private void createSchema() {
        log.warn( "start to create schema" );
        Map<String, List<FieldInformation>> columnInformations = columns.entrySet()
                .stream()
                .collect(
                        Collectors.toMap(
                                table -> EthereumPlugin.HIDDEN_PREFIX + "__" + targetAdapterId + "__" + table.getKey(), // we prepend this to hide the table to the user
                                table -> table.getValue()
                                        .stream()
                                        .map( ExportedColumn::toFieldInformation )
                                        .collect( Collectors.toList() ) ) );

        EventCacheManager.getInstance().createTables( sourceAdapterId, columnInformations, targetAdapterId );
    }


    public void startCaching() {
        log.warn( "start to cache" );
        currentBlock = fromBlock;

        while ( currentBlock.compareTo( toBlock ) <= 0 ) {
            BigInteger endBlock = currentBlock.add( BigInteger.valueOf( batchSizeInBlocks ) );
            if ( endBlock.compareTo( toBlock ) > 0 ) {
                endBlock = toBlock;
            }

            log.warn( "from-to: " + currentBlock + " to " + endBlock ); // in production: instead of .warn take .debug

            for ( Map.Entry<String, EventCache> entry : cache.entrySet() ) {
                String address = entry.getKey();
                EventCache eventCache = entry.getValue();
                try {
                    eventCache.addToCache(address, currentBlock, endBlock, targetAdapterId);
                } catch (CacheException e) {
                    log.error("Error occurred while adding to cache: " + e.getMessage());
                    hasError = true;
                    errorMessage = e.getMessage();
                    throw e;
                } catch (Throwable t) {
                    log.error("Unexpected error during caching: " + t.getMessage(), t);
                    hasError = true;
                    errorMessage = t.getMessage();
                    return;
                }
            }

            currentBlock = endBlock.add( BigInteger.ONE ); // avoid overlapping block numbers
        }
    }


    public CachingStatus getStatus() {
        CachingStatus status = new CachingStatus();
        BigInteger totalBlocks = toBlock.subtract( fromBlock ).add( BigInteger.ONE );
        status.fromBlock = fromBlock;
        status.toBlock = toBlock;
        status.currentBlock = currentBlock;
        status.currentEndBlock = currentBlock.add(BigInteger.valueOf(batchSizeInBlocks));
        status.sourceAdapterId = sourceAdapterId;

        if ( currentBlock.add( BigInteger.valueOf( batchSizeInBlocks ) ).compareTo( toBlock ) > 0 ) {
            status.percent = 100;
            status.state = CachingStatus.ProcessingState.DONE;
            status.currentBlock = null;
            status.currentEndBlock = null;
        } else {
            BigInteger processedBlocks = currentBlock.subtract( fromBlock );
            status.percent = Math.round((processedBlocks.floatValue() / totalBlocks.floatValue() * 100) * 100) / 100f;

            if ( status.percent == 0 ) {
                status.state = CachingStatus.ProcessingState.INITIALIZED;
            } else {
                status.state = CachingStatus.ProcessingState.PROCESSING;
            }
        }

        if (hasError) {
            status.state = ProcessingState.ERROR;
            status.errorMessage = errorMessage;
        }

        return status;
    }

}
