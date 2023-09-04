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
import org.web3j.abi.EventEncoder;
import org.web3j.abi.FunctionReturnDecoder;
import org.web3j.abi.TypeReference;
import org.web3j.abi.datatypes.Event;
import org.web3j.abi.datatypes.Type;
import org.web3j.protocol.Web3j;
import org.web3j.protocol.core.DefaultBlockParameter;
import org.web3j.protocol.core.Response;
import org.web3j.protocol.core.methods.request.EthFilter;
import org.web3j.protocol.core.methods.response.EthLog;
import org.web3j.protocol.core.methods.response.Log;

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
            EventCacheManager.getInstance().writeToStore( event.getCompositeName(), cache.get( event ) ); // write event data into the store
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
            EthLog ethLog = web3j.ethGetLogs( filter ).send(); // Get the EthLog response

            if ( ethLog.hasError() ) {
                Response.Error error = ethLog.getError();
                throw new CacheException( "Error occurred while fetching logs for block range: " + startBlock + " to " + endBlock + ". Please retry starting from block " + startBlock + " and continue to your intended final block. Error Message: " + error.getMessage() );
            }
            List<EthLog.LogResult> rawLogs = ethLog.getLogs();
            List<List<Object>> structuredLogs = normalizeLogs( event, rawLogs );
            cache.put( eventData, structuredLogs );

        } catch ( IOException e ) {
            throw new CacheException( "IO Error fetching logs", e );
        }
    }


    private List<List<Object>> normalizeLogs( Event event, List<EthLog.LogResult> rawLogs ) {
        List<List<Object>> structuredLogs = new ArrayList<>();
        for ( EthLog.LogResult rawLogResult : rawLogs ) {
            Log rawLog = (Log) rawLogResult.get();

            if ( rawLog.getLogIndex() == null ||
                    rawLog.getTransactionIndex() == null ||
                    rawLog.getBlockNumber() == null ) {
                continue; // don't add pending logs because of primary key constraint
            }

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

            // Add other log information
            structuredLog.add( rawLog.isRemoved() );
            structuredLog.add( rawLog.getLogIndex() );
            structuredLog.add( rawLog.getTransactionIndex() );
            structuredLog.add( rawLog.getTransactionHash() );
            structuredLog.add( rawLog.getBlockHash() );
            structuredLog.add( rawLog.getBlockNumber() );
            structuredLog.add( rawLog.getAddress() );

            structuredLogs.add( structuredLog );
        }

        return structuredLogs;
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

