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
import java.util.Arrays;
import java.util.function.Predicate;
import org.web3j.abi.FunctionReturnDecoder;
import org.web3j.abi.datatypes.Type;
import org.web3j.protocol.core.methods.response.EthLog;
import org.web3j.protocol.core.methods.response.Log;
import org.web3j.protocol.core.methods.request.EthFilter;
import org.web3j.protocol.core.DefaultBlockParameter;
import org.web3j.abi.datatypes.Event;
import org.web3j.abi.TypeReference;
import org.web3j.abi.EventEncoder;

public class EventDataReader extends BlockReader {

    private List<EthLog.LogResult> logs;
    private int currentLogIndex = 0;
    private Event event;


    EventDataReader( String clientUrl, int blocks, Predicate<BigInteger> blockNumberPrecate, String contractAddress, BigInteger fromBlock, BigInteger toBlock, Event event ) {
        super( clientUrl, blocks, blockNumberPrecate );
        this.event = event;

        EthFilter filter = new EthFilter(
                DefaultBlockParameter.valueOf( fromBlock ),
                DefaultBlockParameter.valueOf( toBlock ),
                contractAddress
        );

        filter.addSingleTopic( EventEncoder.encode( event ) );

        try {
            logs = web3j.ethGetLogs( filter ).send().getLogs(); // get logs
        } catch ( IOException e ) {
            // Handle exception here. Maybe log an error and re-throw, or set `logs` to an empty list.
        }
    }


    @Override
    public String[] readNext() throws IOException {
        if ( this.blockReads <= 0 || currentLogIndex >= logs.size() ) {
            return null; // no more blocks to read or no more logs to process
        }

        EthLog.LogResult logResult = logs.get( currentLogIndex );
        Log log = (Log) logResult.get();

        currentLogIndex++; // Move to the next log for the next call to readNext()
        if ( currentLogIndex >= logs.size() ) {
            this.blockReads--; // Decrement blockReads when all logs for the current block have been processed
        }

        // Decode the data field of the log
        String data = log.getData();
        List<Type> decodedData = FunctionReturnDecoder.decode( data, event.getNonIndexedParameters() );

        // Decode the topics of the log
        List<String> topics = log.getTopics();
        topics.remove( 0 ); // The first topic is the event signature, so we skip it
        List<Type> decodedTopics = new ArrayList<>();
        for ( int i = 0; i < topics.size(); i++ ) {
            String topic = topics.get( i );
            TypeReference<?> parameterType = event.getIndexedParameters().get( i );
            Type decodedTopic = FunctionReturnDecoder.decodeIndexedValue( topic, parameterType );
            decodedTopics.add( decodedTopic );
        }

        // Combine the decoded topics and data into a single array
        List<Type> allDecodedParameters = new ArrayList<>();
        allDecodedParameters.addAll( decodedTopics );
        allDecodedParameters.addAll( decodedData );

        // Convert the decoded parameters to a String array
        String[] result = new String[allDecodedParameters.size()];
        for ( int i = 0; i < allDecodedParameters.size(); i++ ) {
            result[i] = allDecodedParameters.get( i ).getValue().toString();
        }

        // Add additional columns
        String[] extendedResult = Arrays.copyOf( result, result.length + 7 );
        extendedResult[result.length] = Boolean.toString( log.isRemoved() );
        extendedResult[result.length + 1] = log.getLogIndex().toString();
        extendedResult[result.length + 2] = log.getTransactionIndex().toString();
        extendedResult[result.length + 3] = log.getTransactionHash();
        extendedResult[result.length + 4] = log.getBlockHash();
        extendedResult[result.length + 5] = log.getBlockNumber().toString();
        extendedResult[result.length + 6] = log.getAddress();

        return extendedResult;
    }

}
