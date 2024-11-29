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

package org.polypheny.db.workflow.engine.storage;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;
import org.polypheny.db.PolyImplementation;
import org.polypheny.db.algebra.AlgNode;
import org.polypheny.db.algebra.AlgRoot;
import org.polypheny.db.algebra.core.common.Modify;
import org.polypheny.db.algebra.core.common.Scan;
import org.polypheny.db.algebra.type.AlgDataType;
import org.polypheny.db.catalog.entity.logical.LogicalCollection;
import org.polypheny.db.catalog.entity.logical.LogicalEntity;
import org.polypheny.db.catalog.entity.logical.LogicalGraph;
import org.polypheny.db.catalog.entity.logical.LogicalTable;
import org.polypheny.db.catalog.exceptions.GenericRuntimeException;
import org.polypheny.db.config.RuntimeConfig;
import org.polypheny.db.nodes.Node;
import org.polypheny.db.processing.ImplementationContext;
import org.polypheny.db.processing.ImplementationContext.ExecutedContext;
import org.polypheny.db.processing.Processor;
import org.polypheny.db.processing.QueryContext;
import org.polypheny.db.processing.QueryContext.ParsedQueryContext;
import org.polypheny.db.transaction.Statement;
import org.polypheny.db.type.entity.PolyList;
import org.polypheny.db.type.entity.PolyValue;
import org.polypheny.db.type.entity.category.PolyBlob;
import org.polypheny.db.type.entity.graph.GraphPropertyHolder;
import org.polypheny.db.type.entity.graph.PolyGraph;
import org.polypheny.db.type.entity.graph.PolyPath;
import org.polypheny.db.type.entity.relational.PolyMap;
import org.polypheny.db.util.Pair;

public class QueryUtils {

    private QueryUtils() {

    }


    public static Pair<ParsedQueryContext, AlgRoot> parseAndTranslateQuery( QueryContext context, Statement statement ) {
        ParsedQueryContext parsed = context.getLanguage().parser().apply( context ).get( 0 );
        Processor processor = context.getLanguage().processorSupplier().get();

        if ( parsed.getQueryNode().isEmpty() ) {
            throw new GenericRuntimeException( "Error during parsing of query \"%s\"".formatted( context.getQuery() ) );
        }

        if ( parsed.getLanguage().validatorSupplier() != null ) {
            Pair<Node, AlgDataType> validated = processor.validate(
                    context.getTransactions().get( 0 ),
                    parsed.getQueryNode().get(),
                    RuntimeConfig.ADD_DEFAULT_VALUES_IN_INSERTS.getBoolean() );
            parsed = ParsedQueryContext.fromQuery( parsed.getQuery(), validated.left, parsed );
        }
        AlgRoot root = processor.translate( statement, parsed );
        return Pair.of( parsed, root );
    }


    public static ExecutedContext executeQuery( Pair<ParsedQueryContext, AlgRoot> parsed, Statement statement ) {
        PolyImplementation implementation = statement.getQueryProcessor().prepareQuery( parsed.right, false );
        return new ImplementationContext( implementation, parsed.left, statement, null ).execute( statement );
    }


    public static String quoteAndJoin( List<String> colNames ) {
        return colNames.stream()
                .map( s -> "\"" + s + "\"" )
                .collect( Collectors.joining( ", " ) );
    }


    public static String quotedIdentifier( LogicalEntity entity ) {
        if ( entity instanceof LogicalTable table ) {
            return "\"" + table.getNamespaceName() + "\".\"" + table.getName() + "\"";
        } else if ( entity instanceof LogicalCollection collection ) {
            return "\"" + collection.getNamespaceName() + "\".\"" + collection.getName() + "\"";
        } else if ( entity instanceof LogicalGraph graph ) {
            return "\"" + graph.getNamespaceName() + "\"";
        }
        throw new IllegalArgumentException( "Encountered unknown entity type" );
    }


    public static boolean validateAlg( AlgRoot root, boolean allowDml, List<LogicalEntity> allowedEntities ) {
        Set<Long> allowedIds = allowedEntities == null ? null : allowedEntities.stream().map( e -> e.id ).collect( Collectors.toSet() );
        return validateRecursive( root.alg, allowDml, allowedIds );
    }


    private static boolean validateRecursive( AlgNode root, boolean allowDml, Set<Long> allowedIds ) {
        boolean checkEntities = allowedIds != null;
        if ( !allowDml ) {
            if ( root instanceof Modify ) {
                return false;
            }
        } else if ( checkEntities && root instanceof Modify<?> modify && !allowedIds.contains( modify.entity.id ) ) {
            // check if modify only has allowed entities
            return false;
        }

        // Check Scan
        if ( checkEntities && root instanceof Scan<?> scan && !allowedIds.contains( scan.entity.id ) ) {
            return false;
        }

        return root.getInputs().stream().allMatch( node -> validateRecursive( node, allowDml, allowedIds ) );
    }

    public static class BatchWriter implements AutoCloseable {

        private static final long DEFAULT_BYTE_SIZE = 32; // used as fallback to estimate number of bytes in a PolyValue
        private static final long MAX_BYTES_PER_BATCH = 10 * 1024 * 1024L; // 10 MiB, upper limit to (estimated) size of batch in bytes
        private static final long MAX_TUPLES_PER_BATCH = 10_000; // upper limit to tuples per batch


        private final Map<Long, AlgDataType> paramTypes;
        private final List<Map<Long, PolyValue>> paramValues = new ArrayList<>();
        private long batchSize = -1;

        private final Statement writeStatement;
        private final Pair<ParsedQueryContext, AlgRoot> parsed;


        public BatchWriter( QueryContext context, Statement statement, Map<Long, AlgDataType> paramTypes ) {
            this.writeStatement = statement;
            this.parsed = QueryUtils.parseAndTranslateQuery( context, writeStatement );
            this.paramTypes = paramTypes;
        }


        public void write( Map<Long, PolyValue> valueMap ) {
            if ( batchSize == -1 ) {
                batchSize = computeBatchSize( valueMap.values().toArray( new PolyValue[0] ) );
            }
            paramValues.add( valueMap );

            if ( paramValues.size() < batchSize ) {
                return;
            }
            executeBatch();
        }


        private void executeBatch() {
            int batchSize = paramValues.size();

            writeStatement.getDataContext().setParameterTypes( paramTypes );
            writeStatement.getDataContext().setParameterValues( paramValues );

            // create new implementation for each batch
            ExecutedContext executedContext = QueryUtils.executeQuery( parsed, writeStatement );

            if ( executedContext.getException().isPresent() ) {
                throw new GenericRuntimeException( "An error occurred while writing a batch" );
            }
            List<List<PolyValue>> results = executedContext.getIterator().getAllRowsAndClose();
            long changedCount = results.size() == 1 ? results.get( 0 ).get( 0 ).asLong().longValue() : 0;
            if ( changedCount != batchSize ) {
                throw new GenericRuntimeException( "Unable to write all values of the batch: " + changedCount + " of " + batchSize + " tuples were written" );
            }

            paramValues.clear();
            writeStatement.getDataContext().resetParameterValues();
        }


        @Override
        public void close() throws Exception {
            if ( !paramValues.isEmpty() ) {
                executeBatch();
            }
        }


        //////////////////
        // Static Utils //
        //////////////////
        static long computeBatchSize( PolyValue[] representativeTuple ) {
            long maxFromBytes = MAX_BYTES_PER_BATCH / estimateByteSize( representativeTuple );
            return Math.max( Math.min( maxFromBytes, MAX_TUPLES_PER_BATCH ), 1 );
        }


        private static long estimateByteSize( PolyValue[] tuple ) {
            long size = 0;
            for ( PolyValue value : tuple ) {
                try {
                    size += value.getByteSize().orElse( getFallbackByteSize( value ) );
                } catch ( Exception e ) {
                    size += DEFAULT_BYTE_SIZE;
                }
            }
            return size;
        }


        private static long estimateByteSize( Collection<? extends PolyValue> values ) {
            return estimateByteSize( values.toArray( new PolyValue[0] ) );
        }


        private static long estimateByteSize( PolyMap<? extends PolyValue, ? extends PolyValue> polyMap ) {
            return estimateByteSize( polyMap.getMap().keySet() ) +
                    estimateByteSize( polyMap.getMap().values() );
        }


        private static long getFallbackByteSize( PolyValue value ) {

            return switch ( value.type ) {
                case DATE -> 16L;
                case SYMBOL -> 0L; // ?
                case ARRAY -> {
                    if ( value instanceof PolyList<? extends PolyValue> polyList ) {
                        yield estimateByteSize( polyList.value );
                    }
                    yield DEFAULT_BYTE_SIZE;
                }
                case DOCUMENT, MAP -> {
                    if ( value instanceof PolyMap<? extends PolyValue, ? extends PolyValue> polyMap ) {
                        yield estimateByteSize( polyMap );
                    }
                    yield DEFAULT_BYTE_SIZE;
                }
                case GRAPH -> {
                    if ( value instanceof PolyGraph polyGraph ) {
                        yield estimateByteSize( polyGraph.getNodes() ) + estimateByteSize( polyGraph.getEdges() );
                    }
                    yield DEFAULT_BYTE_SIZE;
                }
                case EDGE, NODE -> {
                    if ( value instanceof GraphPropertyHolder propHolder ) {
                        yield estimateByteSize( propHolder.properties ) + estimateByteSize( propHolder.labels );
                    }
                    yield DEFAULT_BYTE_SIZE;
                }
                case PATH -> {
                    if ( value instanceof PolyPath polyPath ) {
                        yield estimateByteSize( polyPath.getNodes() ) +
                                estimateByteSize( polyPath.getEdges() ) +
                                estimateByteSize( polyPath.getPath() ) +
                                estimateByteSize( polyPath.getNames() ) +
                                estimateByteSize( polyPath.getSegments() );
                    }
                    yield DEFAULT_BYTE_SIZE;
                }
                case FILE -> {
                    if ( value instanceof PolyBlob polyBlob ) {
                        yield polyBlob.value.length;
                    }
                    yield DEFAULT_BYTE_SIZE;
                }
                default -> DEFAULT_BYTE_SIZE;
            };
        }

    }

}
