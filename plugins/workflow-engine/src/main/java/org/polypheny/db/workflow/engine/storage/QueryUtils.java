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

import java.sql.ResultSetMetaData;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;
import org.polypheny.db.PolyImplementation;
import org.polypheny.db.ResultIterator;
import org.polypheny.db.adapter.AdapterManager;
import org.polypheny.db.algebra.AlgNode;
import org.polypheny.db.algebra.AlgRoot;
import org.polypheny.db.algebra.constant.Kind;
import org.polypheny.db.algebra.core.common.Modify;
import org.polypheny.db.algebra.core.common.Scan;
import org.polypheny.db.algebra.type.AlgDataType;
import org.polypheny.db.algebra.type.AlgDataTypeField;
import org.polypheny.db.catalog.Catalog;
import org.polypheny.db.catalog.entity.allocation.AllocationPlacement;
import org.polypheny.db.catalog.entity.logical.LogicalCollection;
import org.polypheny.db.catalog.entity.logical.LogicalColumn;
import org.polypheny.db.catalog.entity.logical.LogicalEntity;
import org.polypheny.db.catalog.entity.logical.LogicalGraph;
import org.polypheny.db.catalog.entity.logical.LogicalIndex;
import org.polypheny.db.catalog.entity.logical.LogicalPrimaryKey;
import org.polypheny.db.catalog.entity.logical.LogicalTable;
import org.polypheny.db.catalog.exceptions.GenericRuntimeException;
import org.polypheny.db.config.RuntimeConfig;
import org.polypheny.db.ddl.DdlManager;
import org.polypheny.db.languages.QueryLanguage;
import org.polypheny.db.nodes.Node;
import org.polypheny.db.processing.ImplementationContext;
import org.polypheny.db.processing.ImplementationContext.ExecutedContext;
import org.polypheny.db.processing.Processor;
import org.polypheny.db.processing.QueryContext;
import org.polypheny.db.processing.QueryContext.ParsedQueryContext;
import org.polypheny.db.transaction.Statement;
import org.polypheny.db.transaction.Transaction;
import org.polypheny.db.transaction.TransactionManagerImpl;
import org.polypheny.db.type.entity.PolyList;
import org.polypheny.db.type.entity.PolyValue;
import org.polypheny.db.type.entity.category.PolyBlob;
import org.polypheny.db.type.entity.graph.GraphPropertyHolder;
import org.polypheny.db.type.entity.graph.PolyGraph;
import org.polypheny.db.type.entity.graph.PolyPath;
import org.polypheny.db.type.entity.relational.PolyMap;
import org.polypheny.db.util.Pair;
import org.polypheny.db.webui.crud.LanguageCrud;
import org.polypheny.db.webui.models.SortState;
import org.polypheny.db.webui.models.catalog.UiColumnDefinition;
import org.polypheny.db.webui.models.catalog.UiColumnDefinition.UiColumnDefinitionBuilder;
import org.polypheny.db.webui.models.requests.UIRequest;
import org.polypheny.db.webui.models.results.QueryType;
import org.polypheny.db.webui.models.results.RelationalResult;
import org.polypheny.db.webui.models.results.Result;

public class QueryUtils {

    private static final long DEFAULT_BYTE_SIZE = 32; // used as fallback to estimate number of bytes in a PolyValue
    private static final String INDEX_NAME = "wf_pk_index";


    private QueryUtils() {

    }


    public static QueryContext constructContext( String query, String queryLanguage, long namespaceId, Transaction transaction ) {
        return QueryContext.builder()
                .query( query )
                .language( QueryLanguage.from( queryLanguage ) )
                .isAnalysed( false )
                .origin( StorageManager.ORIGIN )
                .batch( 100 ) // TODO: ensure this has the desired effect, then change to suitable value
                .namespaceId( namespaceId )
                .transactionManager( transaction.getTransactionManager() )
                .transactions( List.of( transaction ) ).build();
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


    public static ExecutedContext parseAndExecuteQuery( String query, String queryLanguage, long namespaceId, Transaction transaction ) {
        QueryContext context = constructContext( query, queryLanguage, namespaceId, transaction );
        Statement statement = transaction.createStatement();
        return executeQuery( parseAndTranslateQuery( context, statement ), statement );
    }


    public static ExecutedContext executeAlgRoot( AlgRoot root, Statement statement ) {
        QueryContext context = constructContext( "", "SQL", Catalog.defaultNamespaceId, statement.getTransaction() );
        ParsedQueryContext parsedContext = ParsedQueryContext.fromQuery( "", null, context );
        return executeQuery( Pair.of( parsedContext, root ), statement );
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
            return "\"" + Catalog.snapshot().getNamespace( graph.getNamespaceId() ).orElseThrow().getName() + "\"";
        }
        throw new IllegalArgumentException( "Encountered unknown entity type" );
    }


    public static boolean validateAlg( AlgRoot root, boolean allowDml, List<LogicalEntity> allowedEntities ) {
        if ( !(root.kind.belongsTo( Kind.QUERY ) || (allowDml && root.kind.belongsTo( Kind.DML ))) ) {
            return false;
        }

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


    public static List<String> getPkCols( LogicalTable table ) {
        LogicalPrimaryKey pk = Catalog.snapshot().rel().getPrimaryKey( table.primaryKey ).orElseThrow();
        return pk.getFieldNames();
    }


    public static boolean hasIndex( LogicalTable table, List<String> cols ) {
        for ( LogicalIndex index : Catalog.snapshot().rel().getIndexes( table.id, false ) ) {
            System.out.println( index );
            if ( index.key.getFieldNames().equals( cols ) ) {
                return true;
            }
        }
        return false;
    }


    public static void createIndex( LogicalTable table, List<String> cols, boolean isUnique ) {
        System.out.println( "creating index for " + table.getName() );
        AllocationPlacement placement = Catalog.snapshot().alloc().getPlacementsFromLogical( table.id ).get( 0 );

        Transaction indexTransaction = startTransaction( table.getNamespaceId(), "RelIndex" );
        try {
            DdlManager.getInstance().createIndex(
                    table,
                    null,
                    cols,
                    INDEX_NAME,
                    isUnique,
                    AdapterManager.getInstance().getStore( placement.adapterId ).orElse( null ),
                    indexTransaction.createStatement() );
            indexTransaction.commit();
        } catch ( Exception ignored ) {
            // not all adapters support adding indexes (e.g. MonetDB does not)
        }
        System.out.println( "    -> finished!" );
    }


    public static void dropIndex( LogicalTable table, List<String> cols ) {
        for ( LogicalIndex index : Catalog.snapshot().rel().getIndexes( table.id, false ) ) {
            if ( index.name.equals( INDEX_NAME ) && index.key.getFieldNames().equals( cols ) ) {
                Transaction indexTransaction = startTransaction( table.getNamespaceId(), "RelIndexDelete" );
                DdlManager.getInstance().dropIndex( table, index.name, indexTransaction.createStatement() );
                indexTransaction.commit();
                return;
            }
        }
    }


    public static Transaction startTransaction( long namespace ) {
        return startTransaction( namespace, null );
    }


    public static Transaction startTransaction( long namespace, String originSuffix ) {
        String origin = (originSuffix == null || originSuffix.isEmpty()) ? StorageManager.ORIGIN : StorageManager.ORIGIN + "-" + originSuffix;
        return TransactionManagerImpl.getInstance().startTransaction( Catalog.defaultUserId, namespace, false, origin );
    }


    static long computeBatchSize( PolyValue[] representativeTuple, long maxBytesPerBatch, int maxTuplesPerBatch ) {
        long maxFromBytes = maxBytesPerBatch / estimateByteSize( representativeTuple );
        return Math.max( Math.min( maxFromBytes, maxTuplesPerBatch ), 1 );
    }


    public static long estimateByteSize( PolyValue[] tuple ) {
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


    public static Result<?, ?> getRelResult( ExecutedContext context, UIRequest request, Statement statement ) {
        // TODO decide whether to use this method or LanguageCrud directly
        if ( context.getException().isPresent() ) {
            return LanguageCrud.buildErrorResult( statement.getTransaction(), context, context.getException().get() ).build();
        }

        Catalog catalog = Catalog.getInstance();
        ResultIterator iterator = context.getIterator();
        List<List<PolyValue>> rows;
        try {
            rows = iterator.getAllRowsAndClose();
        } catch ( Exception e ) {
            return LanguageCrud.buildErrorResult( statement.getTransaction(), context, e ).build();
        }

        LogicalTable table = null;
        if ( request.entityId != null ) {
            table = Catalog.snapshot().rel().getTable( request.entityId ).orElseThrow();
        }

        List<UiColumnDefinition> header = new ArrayList<>();
        for ( AlgDataTypeField field : context.getIterator().getImplementation().tupleType.getFields() ) {
            String columnName = field.getName();

            UiColumnDefinitionBuilder<?, ?> dbCol = UiColumnDefinition.builder()
                    .name( field.getName() )
                    .dataType( field.getType().getFullTypeString() )
                    .nullable( field.getType().isNullable() == (ResultSetMetaData.columnNullable == 1) )
                    .precision( field.getType().getPrecision() )
                    .sort( new SortState() )
                    .filter( "" );

            // Get column default values
            if ( table != null ) {
                Optional<LogicalColumn> optional = catalog.getSnapshot().rel().getColumn( table.id, columnName );
                if ( optional.isPresent() ) {
                    if ( optional.get().defaultValue != null ) {
                        dbCol.defaultValue( optional.get().defaultValue.value.toJson() );
                    }
                }
            }
            header.add( dbCol.build() );
        }

        List<String[]> data = LanguageCrud.computeResultData( rows, header, statement.getTransaction() );

        return RelationalResult
                .builder()
                .header( header.toArray( new UiColumnDefinition[0] ) )
                .data( data.toArray( new String[0][] ) )
                .dataModel( context.getIterator().getImplementation().getDataModel() )
                .namespace( request.namespace )
                .language( context.getQuery().getLanguage() )
                .affectedTuples( data.size() )
                .queryType( QueryType.from( context.getImplementation().getKind() ) )
                .xid( statement.getTransaction().getXid().toString() )
                .query( context.getQuery().getQuery() ).build();
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
