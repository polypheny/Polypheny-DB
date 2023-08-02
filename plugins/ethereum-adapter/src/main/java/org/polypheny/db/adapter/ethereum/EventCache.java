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


import java.io.IOException;
import java.math.BigInteger;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.concurrent.ConcurrentHashMap;
import java.util.stream.Collectors;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.NotImplementedException;
import org.polypheny.db.adapter.DataSource.ExportedColumn;
import org.polypheny.db.ddl.DdlManager.ColumnTypeInformation;
import org.polypheny.db.ddl.DdlManager.FieldInformation;
import org.web3j.abi.EventEncoder;
import org.web3j.abi.datatypes.Event;
import org.web3j.protocol.Web3j;
import org.web3j.protocol.core.DefaultBlockParameter;
import org.web3j.protocol.core.methods.request.EthFilter;
import org.web3j.protocol.core.methods.response.EthLog;
import org.web3j.protocol.core.methods.response.EthLog.LogResult;
import org.web3j.protocol.http.HttpService;

@Slf4j
public class EventCache {

    private final int batchSizeInBlocks;
    private final Map<Event, List<LogResult<?>>> cache = new ConcurrentHashMap<>(); // a cache for each event
    private final List<Event> events; // maintain a list of events
    private final String smartContractAddress;
    private final BigInteger fromBlock;
    private final BigInteger toBlock;
    protected final Web3j web3j;

    public final int adapterId;
    private final Map<String, List<ExportedColumn>> columns;

    private boolean isCachingStarted = false;


    // Create one instance to handle caching (better for load balancing if we have multiple stores)
    // EventCacheManager is addressed by the Adapter (with registry method)
    // get all the information: adapterId (adapter target name?), threshold, smart contract address, etherscan api key... all the necessary information
    public EventCache( int adapterId, String clientUrl, int batchSizeInBlocks, String smartContractAddress, BigInteger fromBlock, BigInteger toBlock, List<Event> events, Map<String, List<ExportedColumn>> columns ) {
        this.adapterId = adapterId;
        this.columns = columns;
        this.batchSizeInBlocks = batchSizeInBlocks;
        this.smartContractAddress = smartContractAddress;
        this.fromBlock = fromBlock;
        this.toBlock = toBlock;
        this.events = events;
        events.forEach( event -> this.cache.put( event, new ArrayList<>() ) );
        this.web3j = Web3j.build( new HttpService( clientUrl ) );
    }


    public void initializeCaching() {
        // register table in schema
        this.createSchema();
        // start caching
        this.startCaching();
    }


    private void createSchema() {

        Map<String, List<FieldInformation>> columnInformations = columns.entrySet()
                .stream()
                .collect(
                        Collectors.toMap(
                                Entry::getKey,
                                table -> table.getValue()
                                        .stream()
                                        .map( ExportedColumn::toFieldInformation )
                                        .collect( Collectors.toList() ) ) );

        EventCacheManager.getInstance().createTables( adapterId, columnInformations,  );
    }



    public void startCaching() {
        // 1. similiar to getExportedColumn - it only creates a source, but we need one to write it to the store
        // 2. fetch logs from range x to y (chunk defined by threshold) is reached - addToCache
        // 3. write these logs into store - writeToStore
        // 4. Keep going until all the logs are written into the stores
        log.warn( "start to cache" );
        BigInteger currentBlock = fromBlock;

        while ( currentBlock.compareTo( toBlock ) <= 0 ) {
            BigInteger endBlock = currentBlock.add( BigInteger.valueOf( batchSizeInBlocks ) );
            if ( endBlock.compareTo( toBlock ) > 0 ) {
                endBlock = toBlock;
            }

            System.out.println( "from-to: " + currentBlock + " to " + endBlock );

            // for each event fetch logs from block x to block y according to batchSizeInBlocks
            for ( Event event : events ) {
                addToCache( event, currentBlock, endBlock );
            }

            // just another loop for debugging reasons. I will put it in the first loop later on.
            for ( Event event : events ) {
                // if size == 0 skip
                writeToStore( event, "targetStoreEth" ); // write the event into the store
                cache.get( event ).clear(); // clear cache batch
            }

            currentBlock = endBlock.add( BigInteger.ONE ); // avoid overlapping block numbers
        }
    }


    public synchronized void addToCache( Event event, BigInteger startBlock, BigInteger endBlock ) {
        // fetch logs from block x to block y
        // write it into the cache, so it can be written into the store
        EthFilter filter = new EthFilter(
                DefaultBlockParameter.valueOf( startBlock ),
                DefaultBlockParameter.valueOf( endBlock ),
                smartContractAddress
        );

        filter.addSingleTopic( EventEncoder.encode( event ) );

        try {
            List<EthLog.LogResult<?>> logs = web3j.ethGetLogs( filter ).send().getLogs().stream().map( log -> (LogResult<?>) log ).collect( Collectors.toList() );
            // Add fetched logs to cache
            cache.get( event ).addAll( logs );
        } catch ( IOException e ) {
            // Handle exception here. Maybe log an error and re-throw, or set `logs` to an empty list.
        }
    }


    private void writeToStore( Event event, String targetStore ) {
        // write to targetStore
        for ( Event e : events ) {
            // write event into tables (see cacheMap > value)
        }

        // clear the cache (logs)
        cache.get( event ).clear();
    }


    public CachingStatus getStatus() {
        throw new NotImplementedException();
    }

}

