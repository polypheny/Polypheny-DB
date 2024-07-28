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
import org.polypheny.db.prisminterface.utils.PolyValueSerializer;
import org.polypheny.db.prisminterface.utils.PrismUtils;
import org.polypheny.db.type.PolyType;
import org.polypheny.db.type.entity.PolyValue;
import org.polypheny.db.type.entity.graph.PolyEdge;
import org.polypheny.db.type.entity.graph.PolyNode;
import org.polypheny.db.type.entity.graph.PolyPath;
import org.polypheny.prism.ColumnMeta;
import org.polypheny.prism.DocumentFrame;
import org.polypheny.prism.Frame;
import org.polypheny.prism.GraphFrame;
import org.polypheny.prism.ProtoEdge;
import org.polypheny.prism.ProtoNode;
import org.polypheny.prism.ProtoPath;
import org.polypheny.prism.RelationalFrame;

public class StreamingFramework {

    private final int MAX_MESSAGE_SIZE = 1000000000;
    @Getter
    private StreamingIndex index;
    private List<PolyValue> cache;


    public StreamingFramework() {
        this.index = new StreamingIndex();
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
            Frame.Builder frameBuilder,
            SerializedSizeEstimator estimator,
            ResultExtractor<T> extractor,
            FrameAssembler<T> frameModifier
    ) {
        List<T> results = new ArrayList<>();
        int messageSize = 0;
        int fetchedCount = 0;

        if ( hasCachedResult() ) {
            List<PolyValue> cachedResult = getCacheAndClear();
            messageSize += estimator.estimate( cachedResult );
            results.add( extractor.extract( cachedResult ) );
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
            int itemSize = estimator.estimate( currentItem );
            if ( itemSize > MAX_MESSAGE_SIZE ) {
                throw new RuntimeException( "Result is too large to be serialized" );
            }
            if ( messageSize + itemSize < MAX_MESSAGE_SIZE ) {
                results.add( extractor.extract( currentItem ) );
                messageSize += itemSize;
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


    public Frame processGraphResult( ResultIterator iterator, int fetchSize ) {
        return processResult(
                iterator,
                fetchSize,
                Frame.newBuilder(),
                this::estimateGraphSize,
                this::extractGraphData,
                ( frameBuilder, results ) -> {
                    GraphFrame.Builder graphFrameBuilder = GraphFrame.newBuilder();
                    Class<?> elementType = results.get( 0 ).getClass();
                    if ( ProtoNode.class.isAssignableFrom( elementType ) ) {
                        graphFrameBuilder.addAllNodes( (List<ProtoNode>) (Object) results );
                    } else if ( ProtoEdge.class.isAssignableFrom( elementType ) ) {
                        graphFrameBuilder.addAllEdges( (List<ProtoEdge>) (Object) results );
                    } else if ( ProtoPath.class.isAssignableFrom( elementType ) ) {
                        graphFrameBuilder.addAllPaths( (List<ProtoPath>) (Object) results );
                    }
                    frameBuilder.setGraphFrame( graphFrameBuilder.build() );
                }
        );
    }


    private Object extractGraphData( List<PolyValue> polyValues ) {
        PolyType elementType = polyValues.get( 0 ).getType();
        switch ( elementType ) {
            case NODE -> {
                return PolyValueSerializer.buildProtoNode( (PolyNode) (polyValues.get( 0 )), index );
            }
            case EDGE -> {
                return PolyValueSerializer.buildProtoEdge( (PolyEdge) (polyValues.get( 0 )), index );
            }
            case PATH -> {
                return PolyValueSerializer.buildProtoPath( (PolyPath) (polyValues.get( 0 )), index );
            }
            default -> throw new RuntimeException( "Should never be thrown!" );
        }
    }


    private int estimateGraphSize( List<PolyValue> polyValues ) {
        PolyType elementType = polyValues.get( 0 ).getType();
        switch ( elementType ) {
            case NODE -> {
                return SerializationHeuristic.estimateSizeProtoNode( (PolyNode) (polyValues.get( 0 )) );
            }
            case EDGE -> {
                return SerializationHeuristic.estimateSizeProtoEdge( (PolyEdge) (polyValues.get( 0 )) );
            }
            case PATH -> {
                return SerializationHeuristic.estimateSizeProtoPath( (PolyPath) (polyValues.get( 0 )) );
            }
            default -> throw new RuntimeException( "Should never be thrown!" );
        }
    }


    public Frame processRelationalResult( List<ColumnMeta> columnMetas, ResultIterator iterator, int fetchSize ) {
        RelationalFrame.Builder relationalFrameBuilder = RelationalFrame.newBuilder().addAllColumnMeta( columnMetas );

        return processResult(
                iterator,
                fetchSize,
                Frame.newBuilder(),
                SerializationHeuristic::estimateSizeRows,
                result -> PrismUtils.serializeToRow( result, index ),
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
                result -> PolyValueSerializer.buildProtoDocument( result.get( 0 ).asDocument(), index ),
                ( frameBuilder, results ) -> frameBuilder.setDocumentFrame(
                        documentFrameBuilder.addAllDocuments( results ).build()
                )
        );
    }


    @FunctionalInterface
    private interface SerializedSizeEstimator {

        int estimate( List<PolyValue> result );

    }


    @FunctionalInterface
    private interface ResultExtractor<T> {

        T extract( List<PolyValue> result );

    }


    @FunctionalInterface
    private interface FrameAssembler<T> {

        void modifyFrame( Frame.Builder frameBuilder, List<T> results );

    }

}
