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

package org.polypheny.db.prisminterface.streaming;

import java.util.ArrayList;
import java.util.List;
import lombok.Getter;
import org.polypheny.db.ResultIterator;
import org.polypheny.db.prisminterface.ClientConfiguration;
import org.polypheny.db.prisminterface.statements.PIStatement;
import org.polypheny.db.prisminterface.utils.PolyValueSerializer;
import org.polypheny.db.prisminterface.utils.PrismUtils;
import org.polypheny.db.type.entity.PolyValue;
import org.polypheny.prism.ColumnMeta;
import org.polypheny.prism.DocumentFrame;
import org.polypheny.prism.Frame;
import org.polypheny.prism.Frame.Builder;
import org.polypheny.prism.GraphFrame;
import org.polypheny.prism.RelationalFrame;

public class StreamingFramework {

    public static final int MAX_MESSAGE_SIZE = 1000000000;
    public static final int STREAM_LIMIT = 100000000; // 1 MB

    @Getter
    private StreamIndex index;
    private PIStatement statement;
    private List<PolyValue> cache;
    boolean streamingSupported;


    public StreamingFramework( PIStatement statement ) {
        this.index = new StreamIndex();
        this.statement = statement;
        ClientConfiguration config = statement.getClient().getClientConfig();
        this.streamingSupported = config.isSupported( ClientConfiguration.SERVER_STREAMING );
    }


    private StreamingStrategy determineStrategy( Estimate estimate, long messageSize ) {
        if (!streamingSupported) {
            return StreamingStrategy.STREAM_NONE;
        }
        return messageSize + estimate.getDynamicLength() > MAX_MESSAGE_SIZE ? StreamingStrategy.STREAM_ALL : StreamingStrategy.DYNAMIC;
    }


    private static int getSizeForStrategy( Estimate estimate, StreamingStrategy streamingStrategy ) {
        return switch ( streamingStrategy ) {
            case STREAM_ALL -> estimate.getAllStreamedLength();
            case DYNAMIC -> estimate.getDynamicLength();
            case STREAM_NONE -> estimate.getNoneStreamedLength();
        };
    }


    public void reset() {
        index.reset();
    }


    private boolean hasCachedResult() {
        return cache != null;
    }


    private List<PolyValue> getCacheAndClear() {
        List<PolyValue> cachedResult = cache;
        cache = null;
        return cachedResult;
    }


    private <T> Frame processResult(
            ResultIterator iterator,
            int fetchSize,
            Builder frameBuilder,
            SerializedSizeEstimator estimator,
            ResultExtractor<T> extractor,
            FrameAssembler<T> frameModifier
    ) {
        List<T> results = new ArrayList<>();
        int messageSize = 0;
        int fetchedCount = 0;

        if ( hasCachedResult() ) {
            List<PolyValue> cachedResult = getCacheAndClear();
            Estimate estimate = estimator.estimate( cachedResult );
            StreamingStrategy strategy = determineStrategy( estimate, messageSize );
            messageSize += getSizeForStrategy( estimate, strategy );
            results.add( extractor.extract( cachedResult, index, strategy, statement.getId() ) );
            fetchedCount++;
        }

        while ( fetchedCount < fetchSize ) {
            if ( messageSize > MAX_MESSAGE_SIZE ) {
                break;
            }
            List<PolyValue> currentItem = iterator.getNext();
            if ( currentItem == null ) {
                break;
            }
            Estimate estimate = estimator.estimate( currentItem );
            if ( estimate.getAllStreamedLength() > MAX_MESSAGE_SIZE ) {
                throw new RuntimeException( "Result is too large to be serialized" );
            }
            if ( messageSize + estimate.getAllStreamedLength() < MAX_MESSAGE_SIZE ) {
                StreamingStrategy strategy = determineStrategy( estimate, messageSize );
                results.add( extractor.extract( currentItem, index, strategy, statement.getId() ) );
                messageSize += getSizeForStrategy( estimate, strategy );
                fetchedCount++;
            } else {
                cache = currentItem;
                break;
            }
        }

        boolean isLast = !iterator.hasMoreRows();
        frameBuilder.setIsLast( isLast );

        frameModifier.modifyFrame( frameBuilder, results );

        return frameBuilder.build();
    }


    public Frame processRelationalResult( List<ColumnMeta> columnMetas, ResultIterator iterator, int fetchSize ) {
        RelationalFrame.Builder relationalFrameBuilder = RelationalFrame.newBuilder().addAllColumnMeta( columnMetas );

        return processResult(
                iterator,
                fetchSize,
                Frame.newBuilder(),
                SerializationHeuristic::estimateSizeRows,
                PrismUtils::serializeToRow,
                ( frameBuilder, results ) -> frameBuilder.setRelationalFrame(
                        relationalFrameBuilder.addAllRows( results ).build()
                )
        );
    }


    public Frame processDocumentResult( ResultIterator iterator, int fetchSize ) {
        DocumentFrame.Builder documentFrameBuilder = DocumentFrame.newBuilder();

        return processResult(
                iterator,
                fetchSize,
                Frame.newBuilder(),
                result -> SerializationHeuristic.estimateSizeDocument( result.get( 0 ).asDocument() ),
                ( result, index, streamingStrategy, statementId ) -> PolyValueSerializer.buildProtoDocument( result.get( 0 ).asDocument(), index, streamingStrategy, statementId ),
                ( frameBuilder, results ) -> frameBuilder.setDocumentFrame(
                        documentFrameBuilder.addAllDocuments( results ).build()
                )
        );
    }


    public Frame processGraphResult( ResultIterator iterator, int fetchSize ) {
        GraphFrame.Builder graphFrameBuilder = GraphFrame.newBuilder();

        return processResult(
                iterator,
                fetchSize,
                Frame.newBuilder(),
                result -> SerializationHeuristic.estimateSizeGraph( result.get( 0 ) ),
                ( result, index, streamingStrategy, statementId ) -> PolyValueSerializer.buildProtoGraph( result.get( 0 ), index, streamingStrategy, statementId ),
                ( frameBuilder, results ) -> {
                    results.forEach( graphFrameBuilder::addAllElement );
                    frameBuilder.setGraphFrame(
                            graphFrameBuilder.build()
                    );
                }
        );
    }


    @FunctionalInterface
    private interface SerializedSizeEstimator {

        Estimate estimate( List<PolyValue> result );

    }


    @FunctionalInterface
    private interface ResultExtractor<T> {

        T extract( List<PolyValue> result, StreamIndex index, StreamingStrategy streamingStrategy, int statementId );

    }


    @FunctionalInterface
    private interface FrameAssembler<T> {

        void modifyFrame( Builder frameBuilder, List<T> results );

    }

}
