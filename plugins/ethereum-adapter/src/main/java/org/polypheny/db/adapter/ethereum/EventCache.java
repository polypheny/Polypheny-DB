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
import java.util.concurrent.ConcurrentHashMap;
import java.util.stream.Collectors;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.NotImplementedException;
import org.polypheny.db.adapter.DataSource.ExportedColumn;
import org.polypheny.db.ddl.DdlManager.FieldInformation;
import org.polypheny.db.type.PolyType;
import org.web3j.abi.EventEncoder;
import org.web3j.abi.FunctionReturnDecoder;
import org.web3j.abi.TypeReference;
import org.web3j.abi.datatypes.Event;
import org.web3j.abi.datatypes.Type;
import org.web3j.protocol.Web3j;
import org.web3j.protocol.core.DefaultBlockParameter;
import org.web3j.protocol.core.methods.request.EthFilter;
import org.web3j.protocol.core.methods.response.EthLog;
import org.web3j.protocol.core.methods.response.EthLog.LogResult;
import org.web3j.protocol.core.methods.response.Log;
import org.web3j.protocol.http.HttpService;

// TODO evtl.: Man könnte es noch weiter abtrennen. Jedes Event hat einen Cache. Bzw. jedes Event macht sein eigenes caching (hat seine eigen URL)
// könnte evtl. logistisch für Java einfacher sein.
@Slf4j // library to use logging annotations
public class EventCache {

    private final Map<EventData, List<List<Object>>> cache = new ConcurrentHashMap<>(); // a cache for each event
    private final List<EventData> events;
    protected final Web3j web3j;


    public EventCache( List<EventData> events, Web3j web3j ) {
        this.web3j = web3j;
        this.events = events;
        events.forEach( event -> this.cache.put( event, new ArrayList<>() ) );
    }


    public void addToCache( String address, BigInteger startBlock, BigInteger endBlock ) {
        for ( EventData event : events ) {
            addLogsToCache( address, event, startBlock, endBlock );
            if ( cache.get( event ).size() == 0 ) {
                continue;
            }
            EventCacheManager.getInstance().writeToStore( event.getCompositeName(), cache.get( event ) ); // write the event into the store
            cache.get( event ).clear(); // clear cache batch
        }
    }


    private void addLogsToCache( String address, EventData eventData, BigInteger startBlock, BigInteger endBlock ) {
        EthFilter filter = new EthFilter(
                DefaultBlockParameter.valueOf( startBlock ),
                DefaultBlockParameter.valueOf( endBlock ),
                address
        );

        Event event = eventData.getEvent();
        filter.addSingleTopic( EventEncoder.encode( event ) );

        try {
            List<EthLog.LogResult> rawLogs = web3j.ethGetLogs( filter ).send().getLogs();

            List<List<Object>> structuredLogs = new ArrayList<>();

            for ( EthLog.LogResult rawLogResult : rawLogs ) {
                Log rawLog = (Log) rawLogResult.get();
                List<Object> structuredLog = new ArrayList<>();

                // Add all indexed values first (topics)
                for ( int i = 0; i < event.getParameters().size(); i++ ) {
                    TypeReference<?> paramType = event.getParameters().get( i );
                    if ( paramType.isIndexed() ) {
                        structuredLog.add( extractIndexedValue( rawLog, paramType, i ) );
                    }
                }

                // Then add all non-indexed values (data)
                int nonIndexedPosition = 0; // Separate index for non-indexed parameters
                for ( int i = 0; i < event.getParameters().size(); i++ ) {
                    TypeReference<?> paramType = event.getParameters().get( i );
                    if ( !paramType.isIndexed() ) {
                        structuredLog.add( extractNonIndexedValue( rawLog, paramType, nonIndexedPosition, event ) );
                        nonIndexedPosition++;
                    }
                }

                // Add other log information as needed
                structuredLog.add( rawLog.isRemoved() );
                structuredLog.add( rawLog.getLogIndex() );
                structuredLog.add( rawLog.getTransactionIndex() );
                structuredLog.add( rawLog.getTransactionHash() );
                structuredLog.add( rawLog.getBlockHash() );
                structuredLog.add( rawLog.getBlockNumber() );
                structuredLog.add( rawLog.getAddress() );

                structuredLogs.add( structuredLog );
            }

            // If cache is a Map<Event, List<List<Object>>>, you can store structuredLogs as follows
            cache.put( eventData, structuredLogs );

            // We are still writing to memory with logs & .addAll. Right now we will use the memory space.
            //cache.get( event ).addAll( rawLogs );

            // Without using the memory:
            // Directly write to store. How?
            // 1. call getLogs method which returns logs
            // 2. write it directly to the store: writeToStore( getLogs() )
            // This can be done synchronously. David thinks this method is good for my project. This means we don't need the cache Hashmap anymore.

            // or (again using a little bit of memory)
            // use also a hashmap like above, write them into the map (like right now) but this time use multithreading
            // so when one value is put into the map another is written to the store asynchronously

        } catch ( IOException e ) {
            // Handle exception here. Maybe log an error and re-throw, or set `logs` to an empty list.
        }
    }


    private Object extractIndexedValue( Log rawLog, TypeReference<?> paramType, int position ) {
        // Get the indexed parameter from the log based on its position
        String topics = rawLog.getTopics().get( position + 1 ); // The first topic is usually the event signature
        return FunctionReturnDecoder.decodeIndexedValue( topics, paramType );
    }


    private Object extractNonIndexedValue( Log rawLog, TypeReference<?> paramType, int position, Event event ) {
        List<Type> decodedValue = FunctionReturnDecoder.decode( rawLog.getData(), event.getNonIndexedParameters() );
        return decodedValue.get( position );
    }


}

