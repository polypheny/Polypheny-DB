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

package org.polypheny.db.mql2rel;

import com.google.common.collect.ImmutableList;
import java.math.BigDecimal;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.ZoneOffset;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Objects;
import java.util.function.BiFunction;
import java.util.stream.Collectors;
import org.bson.BsonArray;
import org.bson.BsonBoolean;
import org.bson.BsonDocument;
import org.bson.BsonInt32;
import org.bson.BsonInt64;
import org.bson.BsonNumber;
import org.bson.BsonRegularExpression;
import org.bson.BsonString;
import org.bson.BsonValue;
import org.polypheny.db.catalog.Catalog;
import org.polypheny.db.config.RuntimeConfig;
import org.polypheny.db.document.DocumentTypeUtil;
import org.polypheny.db.mql.Mql;
import org.polypheny.db.mql.MqlAggregate;
import org.polypheny.db.mql.MqlCollectionStatement;
import org.polypheny.db.mql.MqlCount;
import org.polypheny.db.mql.MqlDelete;
import org.polypheny.db.mql.MqlFind;
import org.polypheny.db.mql.MqlInsert;
import org.polypheny.db.mql.MqlNode;
import org.polypheny.db.mql.MqlQueryStatement;
import org.polypheny.db.mql.MqlUpdate;
import org.polypheny.db.plan.RelOptCluster;
import org.polypheny.db.plan.RelOptTable;
import org.polypheny.db.prepare.PolyphenyDbCatalogReader;
import org.polypheny.db.processing.MqlProcessor;
import org.polypheny.db.rel.RelCollation;
import org.polypheny.db.rel.RelCollations;
import org.polypheny.db.rel.RelFieldCollation;
import org.polypheny.db.rel.RelFieldCollation.Direction;
import org.polypheny.db.rel.RelNode;
import org.polypheny.db.rel.RelRoot;
import org.polypheny.db.rel.core.AggregateCall;
import org.polypheny.db.rel.core.CorrelationId;
import org.polypheny.db.rel.core.TableModify.Operation;
import org.polypheny.db.rel.logical.LogicalAggregate;
import org.polypheny.db.rel.logical.LogicalDocuments;
import org.polypheny.db.rel.logical.LogicalFilter;
import org.polypheny.db.rel.logical.LogicalProject;
import org.polypheny.db.rel.logical.LogicalSort;
import org.polypheny.db.rel.logical.LogicalTableModify;
import org.polypheny.db.rel.logical.LogicalTableScan;
import org.polypheny.db.rel.type.RelDataType;
import org.polypheny.db.rel.type.RelDataTypeFactory;
import org.polypheny.db.rel.type.RelDataTypeField;
import org.polypheny.db.rex.RexBuilder;
import org.polypheny.db.rex.RexCall;
import org.polypheny.db.rex.RexInputRef;
import org.polypheny.db.rex.RexLiteral;
import org.polypheny.db.rex.RexNode;
import org.polypheny.db.sql.SqlAggFunction;
import org.polypheny.db.sql.SqlKind;
import org.polypheny.db.sql.SqlOperator;
import org.polypheny.db.sql.fun.SqlStdOperatorTable;
import org.polypheny.db.type.PolyType;
import org.polypheny.db.util.ImmutableBitSet;
import org.polypheny.db.util.Pair;

public class MqlToRelConverter {

    private final PolyphenyDbCatalogReader catalogReader;
    private final RelOptCluster cluster;
    private RexBuilder builder;
    private final static Map<String, SqlOperator> mappings;
    private final static List<String> operators;
    private final static Map<String, List<SqlOperator>> gates;
    private final static Map<String, SqlOperator> mathOperators;
    private final static Map<String, SqlOperator> elemMathOperators;
    private static final Map<String, SqlAggFunction> accumulators;
    private final RelDataType any;
    private final RelDataType nullableAny;
    private Map<String, SqlOperator> tempMappings;


    static {
        gates = new HashMap<>();
        gates.put( "$and", Collections.singletonList( SqlStdOperatorTable.AND ) );
        gates.put( "$or", Collections.singletonList( SqlStdOperatorTable.OR ) );
        gates.put( "$nor", Arrays.asList( SqlStdOperatorTable.AND, SqlStdOperatorTable.NOT ) );
        gates.put( "$not", Collections.singletonList( SqlStdOperatorTable.NOT ) );

        mappings = new HashMap<>();

        mappings.put( "$lt", SqlStdOperatorTable.LESS_THAN );
        mappings.put( "$gt", SqlStdOperatorTable.GREATER_THAN );
        mappings.put( "$eq", SqlStdOperatorTable.DOC_EQ );
        mappings.put( "$ne", SqlStdOperatorTable.NOT_EQUALS );
        mappings.put( "$lte", SqlStdOperatorTable.LESS_THAN_OR_EQUAL );
        mappings.put( "$gte", SqlStdOperatorTable.GREATER_THAN_OR_EQUAL );

        mappings.put( "$in", SqlStdOperatorTable.IN );
        mappings.put( "$nin", SqlStdOperatorTable.NOT_IN );

        mappings.put( "$exists", SqlStdOperatorTable.EXISTS );

        mathOperators = new HashMap<>();
        mathOperators.put( "$subtract", SqlStdOperatorTable.MINUS );
        mathOperators.put( "$add", SqlStdOperatorTable.PLUS );
        mathOperators.put( "$multiply", SqlStdOperatorTable.MULTIPLY );
        mathOperators.put( "$divide", SqlStdOperatorTable.DIVIDE );
        mathOperators.put( "$mod", SqlStdOperatorTable.MOD );

        operators = new ArrayList<>();
        operators.addAll( mappings.keySet() );
        operators.addAll( gates.keySet() );
        operators.addAll( mathOperators.keySet() );

        // list/array predicates $elemMatch$

        elemMathOperators = new HashMap<>();

        elemMathOperators.put( "$lt", SqlStdOperatorTable.DOC_ELEM_LT );
        elemMathOperators.put( "$gt", SqlStdOperatorTable.DOC_ELEM_GT );
        elemMathOperators.put( "$eq", SqlStdOperatorTable.DOC_ELEM_EQ );
        elemMathOperators.put( "$ne", SqlStdOperatorTable.DOC_ELEM_NE );
        elemMathOperators.put( "$lte", SqlStdOperatorTable.DOC_ELEM_LTE );
        elemMathOperators.put( "$gte", SqlStdOperatorTable.DOC_ELEM_GTE );

        accumulators = new HashMap<>();
        //$addToSet
        accumulators.put( "$avg", SqlStdOperatorTable.AVG );
        accumulators.put( "$count", SqlStdOperatorTable.COUNT );
        accumulators.put( "$first", SqlStdOperatorTable.FIRST_VALUE );
        accumulators.put( "$last", SqlStdOperatorTable.LAST_VALUE );
        accumulators.put( "$max", SqlStdOperatorTable.MAX );
        //$mergeObjects
        accumulators.put( "$min", SqlStdOperatorTable.MIN );
        //$push
        accumulators.put( "$stdDevPop", SqlStdOperatorTable.STDDEV_POP );
        accumulators.put( "$stdDevSamp", SqlStdOperatorTable.STDDEV_SAMP );
        accumulators.put( "$sum", SqlStdOperatorTable.SUM );

        // special cases
        operators.add( "$literal" );
        operators.add( "$type" );
        operators.add( "$expr" );
        operators.add( "$jsonSchema" );
        operators.add( "$all" );
        operators.add( "$elemMatch" );
        operators.add( "$size" );
    }


    private boolean inQuery = false;
    private boolean excludedId = false;
    private boolean _dataExists = true;
    private boolean attachedJson = false;
    private boolean complexOps;


    public MqlToRelConverter( MqlProcessor mqlProcessor, PolyphenyDbCatalogReader catalogReader, RelOptCluster cluster ) {
        this.catalogReader = catalogReader;
        this.cluster = Objects.requireNonNull( cluster );
        this.any = this.cluster.getTypeFactory().createPolyType( PolyType.ANY );
        this.nullableAny = this.cluster.getTypeFactory().createTypeWithNullability( any, true );
        this.tempMappings = mappings;
    }


    public RelRoot convert( MqlNode query, boolean b, boolean b1 ) {
        if ( query instanceof MqlCollectionStatement ) {
            return convert( (MqlCollectionStatement) query, b, b1 );
        }
        throw new RuntimeException( "DML or DQL need a collection" );
    }


    public RelRoot convert( MqlCollectionStatement query, boolean b, boolean b1 ) {
        Mql.Type kind = query.getKind();
        String dbSchemaName = Catalog.getInstance().getUser( Catalog.defaultUser ).getDefaultSchema().name;
        RelOptTable table = getTable( query, dbSchemaName );
        RelNode node = LogicalTableScan.create( cluster, table );
        this.builder = new RexBuilder( cluster.getTypeFactory() );

        switch ( kind ) {
            case FIND:
                RelNode find = convertFind( (MqlFind) query, table.getRowType(), node );
                return RelRoot.of( find, find.getRowType(), SqlKind.SELECT );
            case COUNT:
                RelNode count = convertCount( (MqlCount) query, table.getRowType(), node );
                return RelRoot.of( count, count.getRowType(), SqlKind.SELECT );
            case AGGREGATE:
                return RelRoot.of( convertAggregate( (MqlAggregate) query, table.getRowType(), node ), SqlKind.SELECT );
            /// dmls
            case INSERT:
                return RelRoot.of( convertInsert( (MqlInsert) query, table ), SqlKind.INSERT );
            case DELETE:
                return RelRoot.of( convertDelete( (MqlDelete) query, table, node ), SqlKind.DELETE );
            case UPDATE:
                return RelRoot.of( convertUpdate( (MqlUpdate) query, table, node ), SqlKind.UPDATE );

            default:
                throw new IllegalStateException( "Unexpected value: " + kind );
        }

    }


    private RelOptTable getTable( MqlCollectionStatement query, String dbSchemaName ) {
        return catalogReader.getTable( ImmutableList.of( dbSchemaName, query.getCollection() ) );
    }


    private RelNode convertUpdate( MqlUpdate query, RelOptTable table, RelNode node ) {
        if ( !query.getQuery().isEmpty() ) {
            node = convertQuery( query, table.getRowType(), node );
            if ( query.isOnlyOne() ) {
                node = wrapLimit( node, 1 );
            }
        }
        if ( query.isUsesPipeline() ) {
            node = convertReducedPipeline( query, table.getRowType(), node );
        } else {
            node = translateUpdate( query, table.getRowType(), node, table );
        }

        return node;
    }


    private RelNode translateUpdate( MqlUpdate query, RelDataType rowType, RelNode node, RelOptTable table ) {
        Map<String, RexNode> updates = new HashMap<>();
        RexNode data = getIdentifier( "_data", rowType, true );
        for ( Entry<String, BsonValue> entry : query.getUpdate().asDocument().entrySet() ) {
            String op = entry.getKey();
            switch ( op ) {
                case ("$currentDate"):
                    updates.putAll( translateCurrentDate( entry.getValue().asDocument(), rowType ) );
                    break;
                default:
                    throw new RuntimeException( "The update operation is not supported." );
            }
        }

        RexCall names = new RexCall(
                cluster.getTypeFactory().createArrayType( cluster.getTypeFactory().createPolyType( PolyType.VARCHAR, 2000 ), updates.keySet().size() ),
                SqlStdOperatorTable.ARRAY_VALUE_CONSTRUCTOR,
                updates.keySet().stream().map( n -> convertLiteral( new BsonString( n ) ) ).collect( Collectors.toList() ) );

        RexCall values = new RexCall(
                cluster.getTypeFactory().createArrayType( cluster.getTypeFactory().createPolyType( PolyType.INTEGER ), updates.values().size() ),
                SqlStdOperatorTable.ARRAY_VALUE_CONSTRUCTOR,
                new ArrayList<>( updates.values() ) );

        RexCall call = new RexCall(
                cluster.getTypeFactory().createPolyType( PolyType.VARCHAR, 2000 ),
                SqlStdOperatorTable.DOC_MERGE_UPDATE, Arrays.asList( data, names, values ) );

        return LogicalTableModify.create(
                table,
                catalogReader,
                node,
                Operation.UPDATE,
                Collections.singletonList( "_data" ),
                Collections.singletonList( call ),
                false );
    }


    private Map<String, RexNode> translateCurrentDate( BsonDocument value, RelDataType rowType ) {
        Map<String, RexNode> updates = new HashMap<>();
        for ( Entry<String, BsonValue> entry : value.entrySet() ) {
            long timeOrDate;
            if ( entry.getValue().isBoolean() ) {
                timeOrDate = LocalDate.now().toEpochDay();
            } else {
                if ( entry.getValue().asDocument().get( "$type" ).asString().getValue().equals( "timestamp" ) ) {
                    timeOrDate = LocalDateTime.now().toEpochSecond( ZoneOffset.UTC );
                } else {
                    timeOrDate = LocalDate.now().toEpochDay();
                }

            }
            updates.put( entry.getKey(), convertLiteral( new BsonInt64( timeOrDate ) ) );
        }
        return updates;
    }


    private RelNode convertReducedPipeline( MqlUpdate query, RelDataType rowType, RelNode node ) {
        return null;
    }


    private RelNode convertDelete( MqlDelete query, RelOptTable table, RelNode node ) {
        RelNode deleteQuery = node;
        if ( !query.getQuery().isEmpty() ) {
            deleteQuery = convertQuery( query, table.getRowType(), node );
            if ( query.isOnlyOne() ) {
                deleteQuery = wrapLimit( node, 1 );
            }
        }
        return LogicalTableModify.create(
                table,
                catalogReader,
                deleteQuery,
                Operation.DELETE,
                null,
                null,
                false );
    }


    private LogicalTableModify convertInsert( MqlInsert query, RelOptTable table ) {
        LogicalTableModify modify = LogicalTableModify.create(
                table,
                catalogReader,
                convertMultipleValues( query.getArray() ),
                Operation.INSERT,
                null,
                null,
                false );
        return modify;
    }


    private RelNode convertCount( MqlCount query, RelDataType rowType, RelNode node ) {
        node = convertQuery( query, rowType, node );

        return LogicalAggregate.create(
                node,
                ImmutableBitSet.of(),
                Collections.singletonList( ImmutableBitSet.of() ),
                Collections.singletonList(
                        AggregateCall.create(
                                SqlStdOperatorTable.COUNT,
                                false,
                                query.isEstimate(),
                                new ArrayList<>(),
                                -1,
                                RelCollations.EMPTY,
                                cluster.getTypeFactory().createPolyType( PolyType.BIGINT ),
                                query.isEstimate() ? "estimatedCount" : "count"
                        ) ) );
    }


    private RelNode convertMultipleValues( BsonArray array ) {
        return LogicalDocuments.create( cluster, ImmutableList.copyOf( array.asArray() ) );
    }


    private Object convertSingleEntry( Entry<String, BsonValue> entry ) {
        if ( entry.getValue().isDocument() ) {
            Map<String, Object> entries = new HashMap<>();
            for ( Entry<String, BsonValue> docEntry : entry.getValue().asDocument().entrySet() ) {
                entries.put( docEntry.getKey(), convertSingleEntry( docEntry ) );
            }
            return entries;
        } else if ( entry.getValue().isArray() ) {
            //handle
            return new ArrayList<>();
        } else {
            RelDataType type = getRelDataType( entry.getValue() );
            Pair<Comparable, PolyType> valuePair = RexLiteral.convertType( getComparable( entry.getValue() ), type );
            return new RexLiteral( valuePair.left, type, valuePair.right );
        }
    }


    private RelDataType getRelDataType( BsonValue value ) {
        PolyType polyType = getPolyType( value );
        switch ( polyType ) {
            case CHAR:
            case BINARY:
            case VARCHAR:
            case VARBINARY:
                return cluster.getTypeFactory().createPolyType( polyType, value.asString().getValue().length() );
            default:
                return cluster.getTypeFactory().createPolyType( getPolyType( value ) );
        }


    }


    private RelNode convertAggregate( MqlAggregate query, RelDataType rowType, RelNode node ) {
        this.excludedId = false;
        boolean updateRowType;
        for ( BsonValue value : query.getPipeline() ) {
            if ( !value.isDocument() && ((BsonDocument) value).size() > 1 ) {
                throw new RuntimeException( "The aggregation pipeline is not used correctly." );
            }
            updateRowType = true;
            switch ( ((BsonDocument) value).getFirstKey() ) {
                case "$match":
                    node = combineFilter( value.asDocument().getDocument( "$match" ), node, rowType );
                    updateRowType = false;
                    break;
                case "$unset":
                case "$project":
                    node = combineProjection( value.asDocument().getDocument( "$project" ), node, rowType, false );
                    break;
                case "$set":
                case "$addFields":
                    node = combineProjection( value.asDocument().getDocument( "$addFields" ), node, rowType, true );
                    break;
                case "$count":
                    node = combineCount( value.asDocument().get( "$count" ), node, rowType );
                    break;
                case "$group":
                    node = combineGroup( value.asDocument().get( "$group" ), node, rowType );
                    break;
                case "$sort":
                    node = combineSort( value.asDocument().get( "$sort" ), node, rowType );
                    break;
                case "$limit":
                    node = combineLimit( value.asDocument().get( "$limit" ), node );
                    break;
                case "$skip":
                    node = combineSkip( value.asDocument().get( "$skip" ), node );
                    break;
                // $unwind
                // todo dl add more pipeline statements
                default:
                    throw new IllegalStateException( "Unexpected value: " + ((BsonDocument) value).getFirstKey() );
            }
            if ( updateRowType ) {
                if ( rowType != null ) {
                    rowType = node.getRowType();
                }
            }
        }

        return node;
    }


    private RelNode combineSkip( BsonValue value, RelNode node ) {
        if ( !value.isNumber() || value.asNumber().intValue() < 0 ) {
            throw new RuntimeException( "$skip pipeline stage needs a positive number after" );
        }

        return LogicalSort.create( node, RelCollations.of(), convertLiteral( value ), null );
    }


    private RelNode combineLimit( BsonValue value, RelNode node ) {
        if ( !value.isNumber() || value.asNumber().intValue() < 0 ) {
            throw new RuntimeException( "$limit pipeline stage needs a positive number after" );
        }

        return LogicalSort.create( node, RelCollations.of(), null, convertLiteral( value ) );
    }


    private RelNode combineSort( BsonValue value, RelNode node, RelDataType rowType ) {
        if ( !value.isDocument() ) {
            throw new RuntimeException( "$sort pipeline stage needs a document after" );
        }

        List<String> names = new ArrayList<>();
        List<Direction> dirs = new ArrayList<>();

        for ( Entry<String, BsonValue> entry : value.asDocument().entrySet() ) {
            if ( !entry.getKey().startsWith( "$" ) ) {
                throw new RuntimeException( "$sort needs references to fields preceding with $" );
            }
            names.add( entry.getKey().substring( 1 ) );
            if ( entry.getValue().asNumber().intValue() == 1 ) {
                dirs.add( Direction.ASCENDING );
            } else {
                dirs.add( Direction.DESCENDING );
            }
        }

        return conditionalWrap( node, rowType, names,
                ( newNode, newRowType ) -> LogicalSort.create( newNode, RelCollations.of( generateCollation( dirs, names, newRowType.getFieldNames() ) ), null, null ) );
    }


    /**
     * This function wraps document fields which are either hidden in the default data field _data or in another parent field
     *
     * @param node
     * @param rowType
     * @param names
     * @param nodeFunction
     * @return
     */
    private RelNode conditionalWrap( RelNode node, RelDataType rowType, List<String> names, BiFunction<RelNode, RelDataType, RelNode> nodeFunction ) {
        // we filter for names which are underlying
        List<String> rowNames = rowType.getFieldNames();
        List<String> hiddenNames = names
                .stream()
                .filter( name -> !rowNames.contains( name.split( "\\." )[0] ) )
                .collect( Collectors.toList() );

        // we generate Json mapping for underlying fields
        List<RexNode> projectionNodes = new ArrayList<>();
        for ( String name : hiddenNames ) {
            RexNode identifier = getIdentifier( name, rowType );
            projectionNodes.add( identifier );
        }

        // those underlying field need to be added via a projection, then the rel operation applied and then removed again
        if ( hiddenNames.size() > 0 ) {

            List<RexNode> nodes = rowType.getFieldList().stream().map( f -> RexInputRef.of( f.getIndex(), rowType ) ).collect( Collectors.toList() );
            nodes.addAll( projectionNodes );
            List<String> nodeNames = rowType.getFieldList().stream().map( RelDataTypeField::getName ).collect( Collectors.toList() );
            nodeNames.addAll( hiddenNames );
            node = LogicalProject.create( node, nodes, nodeNames );

            node = nodeFunction.apply( node, node.getRowType() );

            nodes.removeAll( projectionNodes );
            nodeNames.removeAll( hiddenNames );

            return LogicalProject.create( node, nodes, nodeNames );
        }
        return nodeFunction.apply( node, node.getRowType() );
    }


    private List<RelFieldCollation> generateCollation( List<Direction> dirs, List<String> names, List<String> rowNames ) {
        List<RelFieldCollation> collations = new ArrayList<>();
        int pos = 0;
        int index;
        for ( String name : names ) {
            index = rowNames.indexOf( name );
            collations.add( new RelFieldCollation( index, dirs.get( pos ) ) );
            pos++;
        }
        return collations;
    }


    private RelNode combineGroup( BsonValue value, RelNode node, RelDataType rowType ) {
        if ( !value.isDocument() || !value.asDocument().containsKey( "_id" ) ) {
            throw new RuntimeException( "$group pipeline stage needs a document after, which defines a _id" );
        }

        List<SqlAggFunction> ops = new ArrayList<>();
        List<RexNode> nodes = new ArrayList<>();
        List<String> names = new ArrayList<>();
        List<String> aggNames = new ArrayList<>();

        for ( Entry<String, BsonValue> entry : value.asDocument().entrySet() ) {
            if ( entry.getKey().equals( "_id" ) ) {
                if ( entry.getValue().isNull() ) {
                    names.addAll( 0, rowType.getFieldNames() );
                    nodes.addAll( 0, rowType.getFieldList().stream().map( f -> RexInputRef.of( f.getIndex(), rowType ) ).collect( Collectors.toList() ) );

                } else {
                    names.add( entry.getValue().asString().getValue().substring( 1 ) );
                    nodes.add( getIdentifier( entry.getValue().asString().getValue().substring( 1 ), rowType ) );
                }
            } else {
                if ( !entry.getValue().isDocument() ) {
                    throw new RuntimeException( "$group needs a document with an accumulator and an expression" );
                }
                BsonDocument doc = entry.getValue().asDocument();
                ops.add( accumulators.get( doc.getFirstKey() ) );
                aggNames.add( doc.getFirstKey() );
                names.add( doc.getFirstKey() );
                nodes.add( convertExpression( doc.get( doc.getFirstKey() ), rowType ) );
            }
        }

        node = LogicalProject.create( node, nodes, names );

        return groupBy( value, node, node.getRowType(), aggNames, ops );
    }


    private RexNode convertExpression( BsonValue value, RelDataType rowType ) {
        if ( value.isDocument() ) {
            BsonDocument doc = value.asDocument();
            if ( mathOperators.containsKey( doc.getFirstKey() ) ) {
                return convertMath( doc.getFirstKey(), null, doc.get( doc.getFirstKey() ), rowType, false );
            }

            complexOps = true;
        } else if ( value.isString() ) {
            return getIdentifier( value.asString().getValue().substring( 1 ), rowType );
        }
        return null;
    }


    private RelNode groupBy( BsonValue value, RelNode node, RelDataType rowType, List<String> names, List<SqlAggFunction> aggs ) {
        BsonValue groupBy = value.asDocument().get( "_id" );


        List<AggregateCall> convertedAggs = new ArrayList<>();
        int pos = 0;
        for ( String name : names ) {

            convertedAggs.add(
                    AggregateCall.create(
                            aggs.get( pos ),
                            true,
                            false,
                            Collections.singletonList( rowType.getFieldNames().indexOf( name ) ),
                            -1,
                            RelCollations.EMPTY,
                            cluster.getTypeFactory().createPolyType( PolyType.INTEGER ),
                            name ) );
            pos++;
        }

        if ( !groupBy.isNull() ) {
            String groupName = groupBy.asString().getValue().substring( 1 );
            int index = rowType.getFieldNames().indexOf( groupName );

            node = LogicalAggregate.create(
                    node,
                    ImmutableBitSet.of( index ),
                    Collections.singletonList( ImmutableBitSet.of( index ) ),
                    convertedAggs );
        } else {

            node = LogicalAggregate.create(
                    node,
                    ImmutableBitSet.of(),
                    Collections.singletonList( ImmutableBitSet.of() ),
                    convertedAggs );

        }
        return node;
    }


    private RelNode combineCount( BsonValue value, RelNode node, RelDataType rowType ) {
        if ( value.isString() ) {
            throw new RuntimeException( "$count pipeline stage needs only a string" );
        }
        return LogicalAggregate.create(
                node,
                ImmutableBitSet.of(),
                Collections.singletonList( ImmutableBitSet.of() ),
                Collections.singletonList(
                        AggregateCall.create(
                                SqlStdOperatorTable.COUNT,
                                false,
                                false,
                                new ArrayList<>(),
                                -1,
                                RelCollations.EMPTY,
                                cluster.getTypeFactory().createPolyType( PolyType.BIGINT ),
                                value.asString().getValue()
                        ) ) );
    }


    private RelNode convertFind( MqlFind query, RelDataType rowType, RelNode node ) {
        node = convertQuery( query, rowType, node );

        if ( query.getProjection() != null && !query.getProjection().isEmpty() ) {
            node = combineProjection( query.getProjection(), node, rowType, false );
        }

        if ( query.isOnlyOne() ) {
            node = wrapLimit( node, 1 );
        }

        return node;
    }


    private RelNode convertQuery( MqlQueryStatement query, RelDataType rowType, RelNode node ) {
        if ( query.getQuery() != null && !query.getQuery().isEmpty() ) {
            this.inQuery = true;
            node = combineFilter( query.getQuery(), node, rowType );
        }
        return node;
    }


    private RelNode wrapLimit( RelNode node, int limit ) {
        final RelCollation collation = cluster.traitSet().canonize( RelCollations.of( new ArrayList<>() ) );
        return LogicalSort.create(
                node,
                collation,
                null,
                new RexLiteral(
                        new BigDecimal( limit ),
                        cluster.getTypeFactory()
                                .createPolyType( PolyType.INTEGER ), PolyType.DECIMAL )
        );
    }


    private RelNode combineFilter( BsonDocument filter, RelNode node, RelDataType rowType ) {
        RexNode condition = translateDocument( filter, rowType, null );

        return LogicalFilter.create( node, condition );
    }


    private static Comparable<?> getComparable( BsonValue value ) {
        switch ( value.getBsonType() ) {
            case DOUBLE:
                return value.asDouble().getValue();
            case STRING:
                return value.asString().getValue();
            case DOCUMENT:
                break;
            case ARRAY:
                break;
            case BINARY:
                return value.asBinary().toString();
            case UNDEFINED:
                break;
            case OBJECT_ID:
                break;
            case BOOLEAN:
                return value.asBoolean().getValue();
            case DATE_TIME:
                break;
            case NULL:
                return null;
            case REGULAR_EXPRESSION:
                break;
            case DB_POINTER:
                break;
            case JAVASCRIPT:
                break;
            case SYMBOL:
                break;
            case JAVASCRIPT_WITH_SCOPE:
                break;
            case INT32:
                return value.asInt32().getValue();
            case TIMESTAMP:
                return value.asTimestamp().getValue();
            case INT64:
                return value.asInt64().getValue();
            case DECIMAL128:
                return value.asDecimal128().decimal128Value().bigDecimalValue();
            case MIN_KEY:
                break;
            case MAX_KEY:
                break;
        }
        throw new RuntimeException( "Not implemented Comparable transform" );
    }


    private RexNode convertEntry( String key, String parentKey, BsonValue bsonValue, RelDataType rowType ) {
        List<RexNode> operands = new ArrayList<>();
        if ( !key.startsWith( "$" ) ) {
            return convertField( parentKey == null ? key : parentKey + "." + key, bsonValue, rowType );
        } else {

            if ( operators.contains( key ) ) {
                if ( gates.containsKey( key ) ) {
                    return convertGate( key, parentKey, bsonValue, rowType );
                } else if ( mathOperators.containsKey( key ) ) {

                    // special cases have to id in the array, like $arrayElem
                    boolean losesContext = parentKey != null;
                    RexNode id = null;
                    if ( losesContext ) {
                        // we lose context to the parent and have to "drop it" as we move into $subtract or $eq
                        id = getIdentifier( parentKey, rowType );
                    }
                    RexNode node = convertMath( key, null, bsonValue, rowType, false );

                    if ( losesContext && id != null ) {
                        RelDataType type = cluster.getTypeFactory().createPolyType( PolyType.BOOLEAN );
                        return new RexCall( type, SqlStdOperatorTable.DOC_EQ, Arrays.asList( id, node ) );
                    } else {
                        return node;
                    }

                } else {
                    if ( key.equals( "$exists" ) ) {
                        return convertExists( bsonValue, parentKey, rowType );
                    } else if ( key.equals( "$type" ) ) {
                        return convertType( bsonValue, parentKey, rowType );
                    } else if ( key.equals( "$expr" ) ) {
                        return convertExpr( bsonValue, parentKey, rowType );
                    } else if ( key.equals( "$jsonSchema" ) ) {
                        // jsonSchema is a general match
                        return convertJsonSchema( bsonValue, rowType );
                    } else if ( key.equals( "$all" ) ) {
                        return convertAll( bsonValue, parentKey, rowType );
                    } else if ( key.equals( "$elemMatch" ) ) {
                        return convertElemMatch( bsonValue, parentKey, rowType );
                    } else if ( key.equals( "$size" ) ) {
                        return convertSize( bsonValue, parentKey, rowType );
                    }

                    return translateLogical( key, parentKey, bsonValue, rowType );
                }
            } else {
                // handle others
            }
        }

        return getFixedCall( operands, SqlStdOperatorTable.AND, PolyType.BOOLEAN );
    }


    private RelDataTypeField getDefaultDataField( RelDataType rowType ) {
        return rowType.getField( "_data", false, false );
    }


    private RelDataTypeField getDefaultIdField( RelDataType rowType ) {
        return rowType.getField( "_id", false, false );
    }


    private RexNode convertSlice( String key, BsonValue value, RelDataType rowType, RelDataTypeField field ) {
        BsonNumber skip = new BsonInt32( 0 );
        BsonNumber elements;
        RexNode id = getIdentifier( key, rowType );
        if ( value.isNumber() ) {
            elements = value.asInt32();
        } else if ( value.isArray() && value.asArray().size() == 2 ) {
            skip = value.asArray().get( 0 ).asInt32();
            elements = value.asArray().get( 1 ).asInt32();
        } else {
            throw new RuntimeException( "After a $slice projection a number or an array of 2 is needed" );
        }

        return new RexCall( any, SqlStdOperatorTable.DOC_SLICE, Arrays.asList( id, convertLiteral( skip ), convertLiteral( elements ) ) );
    }


    private RexNode convertArrayAt( String key, BsonValue value, RelDataType rowType, RelDataTypeField field ) {
        String msg = "$arrayElemAt has following structure { $arrayElemAt: [ <array>, <idx> ] }";
        if ( value.isArray() ) {
            List<RexNode> nodes = convertArray( key, (BsonArray) value, true, rowType, msg );
            if ( nodes.size() > 2 ) {
                throw new RuntimeException( msg );
            }
            return new RexCall( any, SqlStdOperatorTable.DOC_ITEM, Arrays.asList( nodes.get( 0 ), nodes.get( 1 ) ) );

        } else {
            throw new RuntimeException( msg );
        }

    }


    private RexNode convertMath( String key, String parentKey, BsonValue bsonValue, RelDataType rowType, boolean isExpr ) {
        if ( key.equals( "$literal" ) ) {
            return convertLiteral( bsonValue );
        }
        SqlOperator op;
        if ( !isExpr ) {
            op = mathOperators.get( key );
        } else {
            op = mappings.get( key );
        }

        String errorMsg = "After a " + String.join( ",", mathOperators.keySet() ) + " a list of literal or documents is needed.";
        if ( bsonValue.isArray() ) {
            List<RexNode> nodes = convertArray( parentKey, bsonValue.asArray(), true, rowType, errorMsg );

            return getFixedCall( nodes, op, isExpr ? PolyType.BOOLEAN : PolyType.ANY );
        } else {
            throw new RuntimeException( errorMsg );
        }
    }


    private RexNode convertGate( String key, String parentKey, BsonValue bsonValue, RelDataType rowType ) {

        SqlOperator op;
        switch ( key ) {
            case "$and":
                op = SqlStdOperatorTable.AND;
                return convertLogicalArray( parentKey, bsonValue, rowType, op, false );
            case "$or":
                op = SqlStdOperatorTable.OR;
                return convertLogicalArray( parentKey, bsonValue, rowType, op, false );
            case "$nor":
                op = SqlStdOperatorTable.OR;
                return convertLogicalArray( parentKey, bsonValue, rowType, op, true );
            case "$not":
                op = SqlStdOperatorTable.NOT;
                if ( bsonValue.isDocument() ) {
                    RelDataType type = cluster.getTypeFactory().createPolyType( PolyType.BOOLEAN );
                    return new RexCall( type, op, Collections.singletonList( translateDocument( bsonValue.asDocument(), rowType, parentKey ) ) );
                } else {
                    throw new RuntimeException( "After a $not a document is needed" );
                }

            default:
                throw new RuntimeException( "This logical operator was not recognized:" );
        }
    }


    private RexNode convertLogicalArray( String parentKey, BsonValue bsonValue, RelDataType rowType, SqlOperator op, boolean isNegated ) {
        String errorMsg = "After logical operators \"$and\",\"$or\" and \"nor\" an array of documents is needed";
        if ( bsonValue.isArray() ) {
            List<RexNode> operands = convertArray( parentKey, bsonValue.asArray(), false, rowType, errorMsg );
            if ( isNegated ) {
                operands = operands.stream().map( this::negate ).collect( Collectors.toList() );
            }
            return getFixedCall( operands, op, PolyType.BOOLEAN );
        } else {
            throw new RuntimeException( errorMsg );
        }
    }


    private List<RexNode> convertArray( String parentKey, BsonArray bsonValue, boolean allowsLiteral, RelDataType rowType, String errorMsg ) {
        List<RexNode> operands = new ArrayList<>();
        for ( BsonValue value : bsonValue ) {
            if ( value.isDocument() ) {
                operands.add( translateDocument( value.asDocument(), rowType, parentKey ) );
            } else if ( value.isString() && value.asString().getValue().startsWith( "$" ) ) {
                operands.add( getIdentifier( value.asString().getValue().substring( 1 ), rowType ) );
            } else if ( allowsLiteral ) {
                operands.add( convertLiteral( value ) );
            } else {
                throw new RuntimeException( errorMsg );
            }
        }
        return operands;
    }


    private RexNode convertField( String parentKey, BsonValue bsonValue, RelDataType rowType ) {

        if ( bsonValue.isDocument() ) {
            // we have a document where the sub-keys are either logical like "$eq, $or"
            // we don't attach the id yet as we maybe have sub-documents
            return translateDocument( bsonValue.asDocument(), rowType, parentKey );
        } else {
            // we have a simple assignment to a value, can attach and translate the value
            List<RexNode> nodes = new ArrayList<>();
            nodes.add( getIdentifier( parentKey, rowType ) );

            if ( bsonValue.isArray() ) {
                List<RexNode> arr = convertArray( parentKey, bsonValue.asArray(), true, rowType, "" );
                nodes.add( getArray( arr,
                        cluster.getTypeFactory().createArrayType( nullableAny, arr.size() ) ) );
            } else {
                nodes.add( convertLiteral( bsonValue ) );
            }

            return getFixedCall( nodes, SqlStdOperatorTable.DOC_EQ, PolyType.BOOLEAN );
        }

    }


    private RexNode getIdentifier( String parentKey, RelDataType rowType ) {
        return getIdentifier( parentKey, rowType, false );
    }


    private RexNode getIdentifier( String parentKey, RelDataType rowType, boolean useAccess ) {
        List<String> rowNames = rowType.getFieldNames();
        if ( rowNames.contains( parentKey ) ) {
            if ( useAccess ) {
                return attachCorrel( parentKey, rowType );
            } else {
                return attachRef( parentKey, rowType );
            }
        } else if ( rowNames.contains( parentKey.split( "\\." )[0] ) ) {
            this.attachedJson = true;
            return translateJsonValue( rowNames.indexOf( parentKey.split( "\\." )[0] ), rowType, parentKey, useAccess );
        } else if ( _dataExists ) {
            // the default _data field does still exist
            this.attachedJson = true;
            return translateJsonValue( getDefaultDataField( rowType ).getIndex(), rowType, parentKey, useAccess );
        } else {
            return null;
        }

    }


    private RexNode attachRef( String parentKey, RelDataType rowType ) {
        RelDataTypeField field = rowType.getField( parentKey, false, false );
        return RexInputRef.of( field.getIndex(), rowType );
    }


    private RexNode attachCorrel( String parentKey, RelDataType rowType ) {
        RelDataTypeField field = rowType.getField( parentKey, false, false );
        return builder.makeCorrel( rowType, new CorrelationId( field.getIndex() ) );
    }


    private RexNode getFixedCall( List<RexNode> operands, SqlOperator op, PolyType polyType ) {
        if ( operands.size() == 1 ) {
            if ( op.kind == SqlKind.NOT && operands.get( 0 ) instanceof RexCall && ((RexCall) operands.get( 0 )).op.kind == SqlKind.NOT ) {
                // we have a nested NOT, which can be removed
                return ((RexCall) operands.get( 0 )).operands.get( 0 );
            }

            return operands.get( 0 );
        } else {
            List<RexNode> toRemove = new ArrayList<>();
            List<RexNode> toAdd = new ArrayList<>();
            // maybe we have to fix nested AND or OR combinations
            for ( RexNode operand : operands ) {
                if ( operand instanceof RexCall && ((RexCall) operand).op.getName().equals( op.getName() ) ) { // TODO DL maybe remove if not longer same type
                    toAdd.addAll( ((RexCall) operand).operands );
                    toRemove.add( operand );
                }
            }
            if ( toAdd.size() > 0 ) {
                operands.addAll( toAdd );
                operands.removeAll( toRemove );
            }

            return new RexCall( cluster.getTypeFactory().createTypeWithNullability( cluster.getTypeFactory().createPolyType( polyType ), true ), op, operands );
        }
    }


    private RexNode translateDocument( BsonDocument bsonDocument, RelDataType rowType, String parentKey ) {
        ArrayList<RexNode> operands = new ArrayList<>();

        if ( bsonDocument.getFirstKey().equals( "$regex" ) ) {
            operands.add( convertRegex( bsonDocument, parentKey, rowType ) );
        }

        for ( Entry<String, BsonValue> entry : bsonDocument.entrySet() ) {
            if ( entry.getKey().equals( "$regex" ) ) {
                operands.add( convertRegex( bsonDocument, parentKey, rowType ) );
            } else if ( !entry.getKey().equals( "$options" ) ) {
                // normal handling
                operands.add( convertEntry( entry.getKey(), parentKey, entry.getValue(), rowType ) );
            }
        }
        return getFixedCall( operands, SqlStdOperatorTable.AND, PolyType.BOOLEAN );
    }


    private RexNode translateLogical( String key, String parentKey, BsonValue bsonValue, RelDataType rowType ) {
        SqlOperator op;
        List<RexNode> nodes = new ArrayList<>();
        op = mappings.get( key );
        switch ( op.kind ) {
            case IN:
            case NOT_IN:
                return convertIn( bsonValue, op, parentKey, rowType );
            default:
                if ( parentKey != null ) {
                    nodes.add( getIdentifier( parentKey, rowType ) );
                }
                nodes.add( convertLiteral( bsonValue ) );
                return new RexCall( cluster.getTypeFactory().createPolyType( PolyType.BOOLEAN ), op, nodes );
        }

    }


    private RexNode convertRegex( BsonDocument bsonDocument, String parentKey, RelDataType rowType ) {
        String options = "";
        if ( bsonDocument.size() == 2 && bsonDocument.containsKey( "$regex" ) && bsonDocument.containsKey( "$options" ) ) {
            options = bsonDocument.get( "$options" ).isString() ? bsonDocument.get( "$options" ).asString().getValue() : "";
        }
        BsonValue regex = bsonDocument.get( "$regex" );

        String stringRegex;
        if ( regex.isString() ) {
            stringRegex = regex.asString().getValue();
        } else if ( regex.isRegularExpression() ) {
            BsonRegularExpression bson = regex.asRegularExpression();
            stringRegex = bson.getPattern();
            options += bson.getOptions();
        } else {
            throw new RuntimeException( "$regex needs to be either a regular expression or a string" );
        }

        return getRegex( stringRegex, options, parentKey, rowType );
    }


    private RexCall getRegex( String stringRegex, String options, String parentKey, RelDataType rowType ) {
        return new RexCall(
                cluster.getTypeFactory().createPolyType( PolyType.BOOLEAN ),
                SqlStdOperatorTable.DOC_REGEX_MATCH,
                Arrays.asList(
                        getIdentifier( parentKey, rowType ),
                        convertLiteral( new BsonString( stringRegex ) ),
                        convertLiteral( new BsonBoolean( options.contains( "i" ) ) ),
                        convertLiteral( new BsonBoolean( options.contains( "m" ) ) ),
                        convertLiteral( new BsonBoolean( options.contains( "x" ) ) ),
                        convertLiteral( new BsonBoolean( options.contains( "s" ) ) )
                ) );
    }


    private RexNode convertExists( BsonValue value, String parentKey, RelDataType rowType ) {
        if ( value.isBoolean() ) {

            int index = getIndexOfParentField( parentKey, rowType );

            RexNode exists = translateJsonExists( index, rowType, parentKey );
            if ( !value.asBoolean().getValue() ) {
                return negate( exists );
            }
            return exists;

        } else {
            throw new RuntimeException( "$exist without a boolean is not supported" );
        }
    }


    private int getIndexOfParentField( String parentKey, RelDataType rowType ) {
        int index = rowType.getFieldNames().indexOf( parentKey );
        if ( index < 0 ) {
            index = rowType.getFieldNames().indexOf( parentKey.split( "\\." )[0] );
            if ( index < 0 ) {
                if ( _dataExists ) {
                    index = getDefaultDataField( rowType ).getIndex();
                } else {
                    throw new RuntimeException( "The field does not exist in the collection" );
                }
            }
        }
        return index;
    }


    private RexNode convertExpr( BsonValue bsonValue, String parentKey, RelDataType rowType ) {
        if ( bsonValue.isDocument() && bsonValue.asDocument().size() == 1 ) {
            BsonDocument doc = bsonValue.asDocument();
            return convertMath( doc.getFirstKey(), parentKey, doc.get( doc.getFirstKey() ), rowType, true );

        } else {
            throw new RuntimeException( "After $expr there needs to be a document with a single entry" );
        }
    }


    private RexNode convertJsonSchema( BsonValue bsonValue, RelDataType rowType ) {
        if ( bsonValue.isDocument() ) {
            return new RexCall( nullableAny, SqlStdOperatorTable.DOC_JSON_MATCH, Collections.singletonList( RexInputRef.of( getIndexOfParentField( "_data", rowType ), rowType ) ) );
        } else {
            throw new RuntimeException( "After $jsonSchema there needs to follow a document" );
        }
    }


    private RexNode convertSize( BsonValue bsonValue, String parentKey, RelDataType rowType ) {
        if ( bsonValue.isNumber() ) {
            RexNode id = getIdentifier( parentKey, rowType );
            return new RexCall( cluster.getTypeFactory().createPolyType( PolyType.BOOLEAN ), SqlStdOperatorTable.DOC_SIZE_MATCH, Arrays.asList( id, convertLiteral( bsonValue ) ) );
        } else {
            throw new RuntimeException( "After $size there needs to follow a number" );
        }

    }


    private RexNode convertElemMatch( BsonValue bsonValue, String parentKey, RelDataType rowType ) {
        if ( !bsonValue.isDocument() ) {
            throw new RuntimeException( "After $elemMatch there needs to follow a document" );
        }

        RexCall op = new RexCall( cluster.getTypeFactory().createPolyType( PolyType.BOOLEAN ), SqlStdOperatorTable.EQUALS, Arrays.asList( convertLiteral( new BsonInt32( 32 ) ), convertLiteral( new BsonInt32( 32 ) ) ) );

        return null;
    }


    private RexNode convertAll( BsonValue bsonValue, String parentKey, RelDataType rowType ) {
        RelDataType type = cluster.getTypeFactory().createTypeWithNullability( cluster.getTypeFactory().createPolyType( PolyType.BOOLEAN ), true );
        if ( bsonValue.isArray() ) {
            List<RexNode> arr = convertArray( parentKey, bsonValue.asArray(), true, rowType, "" );
            RexNode id = getIdentifier( parentKey, rowType );

            List<RexNode> operands = new ArrayList<>();
            for ( RexNode rexNode : arr ) {
                operands.add( new RexCall( type, SqlStdOperatorTable.DOC_EQ, Arrays.asList( id, rexNode ) ) );
            }

            return getFixedCall( operands, SqlStdOperatorTable.AND, PolyType.BOOLEAN );
        } else {
            throw new RuntimeException( "After $all there needs to follow a array" );
        }

    }


    private RexNode convertType( BsonValue value, String parentKey, RelDataType rowType ) {
        String errorMsg = "$type needs either a array of type names or numbers or a single number";
        RexCall types;
        if ( value.isArray() ) {
            List<Integer> numbers = new ArrayList<>();
            for ( BsonValue bsonValue : value.asArray() ) {
                if ( bsonValue.isString() || bsonValue.isNumber() ) {
                    numbers.add( bsonValue.isNumber() ? bsonValue.asNumber().intValue() : DocumentTypeUtil.getTypeNumber( bsonValue.asString().getValue() ) );
                } else {
                    throw new RuntimeException( errorMsg );
                }
            }
            types = getIntArray( numbers );
        } else if ( value.isNumber() || value.isString() ) {
            int typeNumber = value.isNumber() ? value.asNumber().intValue() : DocumentTypeUtil.getTypeNumber( value.asString().getValue() );
            types = getIntArray( Collections.singletonList( typeNumber ) );
        } else {
            throw new RuntimeException( errorMsg );
        }
        return new RexCall(
                cluster.getTypeFactory().createPolyType( PolyType.BOOLEAN ),
                SqlStdOperatorTable.DOC_TYPE_MATCH,
                Arrays.asList( getIdentifier( parentKey, rowType ), types ) );

    }


    private RexNode negate( RexNode exists ) {
        return new RexCall( cluster.getTypeFactory().createPolyType( PolyType.BOOLEAN ), SqlStdOperatorTable.NOT, Collections.singletonList( exists ) );
    }


    private RexNode translateJsonExists( int index, RelDataType rowType, String key ) {
        RelDataTypeFactory factory = cluster.getTypeFactory();
        RelDataType type = factory.createPolyType( PolyType.ANY );
        RelDataType anyType = factory.createTypeWithNullability( type, true );

        RexCall common = getJsonCommonApi( index, rowType, key, anyType );

        return new RexCall(
                factory.createPolyType( PolyType.BOOLEAN ),
                SqlStdOperatorTable.JSON_EXISTS,
                Collections.singletonList( common ) );
    }


    private RexNode translateJsonValue( int index, RelDataType rowType, String key, boolean useAccess ) {

        RexCall filter = getStringArray( Arrays.asList( key.split( "\\." ) ) );
        return new RexCall(
                any,
                SqlStdOperatorTable.DOC_QUERY_VALUE,
                Arrays.asList(
                        useAccess
                                ? builder.makeCorrel( rowType, new CorrelationId( index ) )
                                : RexInputRef.of( index, rowType ),
                        filter ) );

    }


    private RexNode translateJsonQuery( int index, RelDataType rowType, String key, List<String> excludes ) {
        RexCall filter = getNestedArray( excludes.stream().map( e -> Arrays.asList( e.split( "\\." ) ) ).collect( Collectors.toList() ) );
        return new RexCall( any, SqlStdOperatorTable.DOC_QUERY_EXCLUDE, Arrays.asList( RexInputRef.of( index, rowType ), filter ) );
    }


    private RexCall getJsonCommonApi( int index, RelDataType rowType, String key, RelDataType anyType ) {
        return getJsonCommonApi( index, rowType, key, anyType, new ArrayList<>() );
    }


    private RexCall getJsonCommonApi( int index, RelDataType rowType, String key, RelDataType anyType, List<String> excludes ) {
        RexCall ref;
        if ( excludes.size() > 0 ) {
            RexCall excludesCall = getStringArray( excludes );
            ref = new RexCall(
                    anyType,
                    SqlStdOperatorTable.JSON_VALUE_EXPRESSION_EXCLUDED,
                    Arrays.asList( RexInputRef.of( index, rowType ), excludesCall ) );
        } else {
            ref = new RexCall(
                    anyType,
                    SqlStdOperatorTable.JSON_VALUE_EXPRESSION,
                    Collections.singletonList( RexInputRef.of( index, rowType ) ) );
        }
        String jsonFilter = RuntimeConfig.JSON_MODE.getString() + (key == null ? " $" : " $." + key);
        return new RexCall( anyType, SqlStdOperatorTable.JSON_API_COMMON_SYNTAX, Arrays.asList( ref, convertLiteral( new BsonString( jsonFilter ) ) ) );
    }


    private RexCall getNestedArray( List<List<String>> lists ) {
        List<RexNode> nodes = new ArrayList<>();
        for ( List<String> list : lists ) {
            nodes.add( getStringArray( list ) );
        }

        return new RexCall(
                cluster.getTypeFactory().createArrayType( cluster.getTypeFactory().createArrayType(
                        cluster.getTypeFactory().createPolyType( PolyType.CHAR, 200 ),
                        -1 ), nodes.size() ),
                SqlStdOperatorTable.ARRAY_VALUE_CONSTRUCTOR, nodes );
    }


    private RexCall getIntArray( List<Integer> elements ) {
        List<RexNode> rexNodes = new ArrayList<>();
        for ( Integer name : elements ) {
            rexNodes.add( convertLiteral( new BsonInt32( name ) ) );
        }

        RelDataType type = cluster.getTypeFactory().createArrayType(
                cluster.getTypeFactory().createPolyType( PolyType.INTEGER ),
                rexNodes.size() );
        return getArray( rexNodes, type );
    }


    private RexCall getStringArray( List<String> elements ) {
        List<RexNode> rexNodes = new ArrayList<>();
        int maxSize = 0;
        for ( String name : elements ) {
            rexNodes.add( convertLiteral( new BsonString( name ) ) );
            maxSize = Math.max( name.length(), maxSize );
        }

        RelDataType type = cluster.getTypeFactory().createArrayType(
                cluster.getTypeFactory().createPolyType( PolyType.CHAR, maxSize ),
                rexNodes.size() );
        return getArray( rexNodes, type );
    }


    private RexCall getArray( List<RexNode> elements, RelDataType type ) {

        return new RexCall( type, SqlStdOperatorTable.ARRAY_VALUE_CONSTRUCTOR, elements );
    }


    private RexNode convertLiteral( BsonValue bsonValue ) {
        RelDataType type = getRelDataType( bsonValue );
        Pair<Comparable, PolyType> valuePair = RexLiteral.convertType( getComparable( bsonValue ), type );
        return new RexLiteral( valuePair.left, type, valuePair.right );
    }


    private RexNode convertIn( BsonValue bsonValue, SqlOperator op, String key, RelDataType rowType ) {
        RelDataType type = cluster.getTypeFactory().createTypeWithNullability( cluster.getTypeFactory().createPolyType( PolyType.BOOLEAN ), true );

        List<RexNode> operands = new ArrayList<>();
        boolean isIn = op == SqlStdOperatorTable.IN;
        op = isIn ? SqlStdOperatorTable.OR : SqlStdOperatorTable.AND;
        RexNode id = getIdentifier( key, rowType );

        for ( BsonValue literal : bsonValue.asArray() ) {
            if ( literal.isDocument() ) {
                throw new RuntimeException( "Non-literal in $in clauses are not supported" );
            }
            if ( literal.isRegularExpression() ) {
                RexNode filter = getRegex( literal.asRegularExpression().getPattern(), literal.asRegularExpression().getOptions(), key, rowType );
                if ( !isIn ) {
                    filter = negate( filter );
                }
                operands.add( filter );
            } else {
                operands.add( new RexCall( type, isIn ? SqlStdOperatorTable.DOC_EQ : SqlStdOperatorTable.NOT_EQUALS, Arrays.asList( id, convertLiteral( literal ) ) ) );
            }
        }

        return getFixedCall( operands, op, PolyType.BOOLEAN );
        /*if ( operands.size() == 1 ) {
            return operands.get( 0 );
        }

        return new RexCall( type, op, operands );*/
    }


    private PolyType getPolyType( BsonValue bsonValue ) {
        switch ( bsonValue.getBsonType() ) {

            case END_OF_DOCUMENT:
                break;
            case DOUBLE:
                return PolyType.DOUBLE;
            case STRING:
                return PolyType.CHAR;
            case DOCUMENT:
                return PolyType.JSON;
            case ARRAY:
                break;
            case BINARY:
                return PolyType.BINARY;
            case UNDEFINED:
                break;
            case OBJECT_ID:
                break;
            case BOOLEAN:
                return PolyType.BOOLEAN;
            case DATE_TIME:
                return PolyType.BIGINT;
            case NULL:
                return PolyType.NULL;
            case REGULAR_EXPRESSION:
                break;
            case DB_POINTER:
                break;
            case JAVASCRIPT:
                break;
            case SYMBOL:
                break;
            case JAVASCRIPT_WITH_SCOPE:
                break;
            case INT32:
                return PolyType.INTEGER;
            case TIMESTAMP:
                return PolyType.BIGINT;
            case INT64:
                return PolyType.BIGINT;
            case DECIMAL128:
                return PolyType.DECIMAL;
            case MIN_KEY:
                break;
            case MAX_KEY:
                break;
        }
        throw new RuntimeException( "Not implemented " );
    }


    private RelNode combineProjection( BsonDocument projection, RelNode node, RelDataType rowType, boolean isAddFields ) {
        Map<String, RexNode> includes = new HashMap<>();
        List<String> excludes = new ArrayList<>();

        for ( Entry<String, BsonValue> entry : projection.entrySet() ) {
            BsonValue value = entry.getValue();
            if ( value.isNumber() && !isAddFields ) {
                // we have a simple projection; [name]: 1 (include) or [name]:0 (exclude)
                RelDataTypeField field = getTypeFieldOrDefault( rowType, entry.getKey() );

                if ( value.asNumber().intValue() == 1 ) {
                    includes.put( entry.getKey(), getIdentifier( entry.getKey(), rowType ) );
                } else if ( value.asNumber().intValue() == 0 ) {
                    excludes.add( entry.getKey() );
                }

            } else if ( value.isString() && value.asString().getValue().startsWith( "$" ) ) {
                // we have a renaming; [new name]: $[old name] ( this counts as a inclusion projection
                String oldName = value.asString().getValue().substring( 1 );
                RelDataTypeField field = getTypeFieldOrDefault( rowType, oldName );

                includes.put( entry.getKey(), getIdentifier( oldName, rowType ) );
            } else if ( value.isDocument() && value.asDocument().size() == 1 && value.asDocument().getFirstKey().startsWith( "$" ) ) {
                String func = value.asDocument().getFirstKey();
                RelDataTypeField field = getDefaultDataField( rowType );
                if ( mathOperators.containsKey( func ) ) {
                    includes.put( entry.getKey(), convertMath( func, entry.getKey(), value.asDocument().get( func ), rowType, false ) );
                } else if ( func.equals( "$arrayElemAt" ) ) {
                    includes.put( entry.getKey(), convertArrayAt( entry.getKey(), value.asDocument().get( func ), rowType, field ) );
                } else if ( func.equals( "$slice" ) ) {
                    includes.put( entry.getKey(), convertSlice( entry.getKey(), value.asDocument().get( func ), rowType, field ) );
                }
            } else if ( isAddFields && !value.isDocument() ) {
                if ( value.isArray() ) {
                    List<RexNode> nodes = new ArrayList<>();
                    for ( BsonValue bsonValue : value.asArray() ) {
                        nodes.add( convertLiteral( bsonValue ) );
                    }
                    includes.put( entry.getKey(), getArray( nodes, any ) );
                } else {
                    includes.put( entry.getKey(), convertLiteral( value ) );
                }

            } else {
                String msg;
                if ( !isAddFields ) {
                    msg = "After a projection there needs to be either a number, a renaming, a literal or a function.";
                } else {
                    msg = "After a projection there needs to be either a renaming, a literal or a function.";
                }
                throw new RuntimeException( msg );
            }

        }

        if ( includes.size() != 0 && excludes.size() != 0 ) {
            throw new RuntimeException( "Include projection and exclude projections are not possible at the same time." );
        }

        if ( excludes.size() > 0 ) {
            if ( _dataExists ) {
                // we have still the _data field, where the fields need to be extracted
                // exclusion projections only work for the underlying _data field
                RelDataTypeField defaultDataField = getDefaultDataField( rowType );

                List<RexNode> values = new ArrayList<>( Collections.singletonList( translateJsonQuery( defaultDataField.getIndex(), rowType, null, excludes ) ) );
                List<String> names = new ArrayList<>( Collections.singletonList( defaultDataField.getName() ) );

                // we only need to do this if it is the second time
                if ( !excludes.contains( "_id" ) && !excludedId ) {
                    names.add( 0, "_id" );
                    values.add( 0, RexInputRef.of( 0, rowType ) );
                }

                if ( excludes.contains( "_id" ) ) {
                    this.excludedId = true;
                }

                return LogicalProject.create( node, values, names );
            } else {
                // we already projected the _data field away and have to work with what we got
                List<RexNode> values = new ArrayList<>();
                List<String> names = new ArrayList<>();

                for ( RelDataTypeField field : rowType.getFieldList() ) {
                    if ( !excludes.contains( field.getName() ) ) {
                        names.add( field.getName() );
                        values.add( RexInputRef.of( field.getIndex(), rowType ) );
                    }
                }

                return LogicalProject.create( node, values, names );
            }
        } else if ( isAddFields && _dataExists ) {
            List<String> names = rowType.getFieldNames();

            // we have to implement the added fields into the _data field
            // as this is later used to retrieve them when projecting

            int dataIndex = rowType.getFieldNames().indexOf( "_data" );

            for ( Entry<String, RexNode> entry : includes.entrySet() ) {
                List<RexNode> values = new ArrayList<>();
                for ( RelDataTypeField field : rowType.getFieldList() ) {
                    if ( field.getIndex() != dataIndex ) {
                        values.add( RexInputRef.of( field.getIndex(), rowType ) );
                    } else {
                        // we attach the new values to the input bson
                        values.add( new RexCall(
                                any,
                                SqlStdOperatorTable.DOC_ADD_FIELDS,
                                Arrays.asList(
                                        RexInputRef.of( dataIndex, rowType ),
                                        convertLiteral( new BsonString( entry.getKey() ) ),
                                        entry.getValue() ) ) );
                    }
                }

                node = LogicalProject.create( node, values, names );
            }

            return node;
        } else if ( includes.size() > 0 ) {
            List<RexNode> values = new ArrayList<>( includes.values() );
            List<String> names = new ArrayList<>( includes.keySet() );

            if ( !includes.containsKey( "_id" ) && !excludedId ) {
                names.add( 0, "_id" );
                values.add( 0, RexInputRef.of( 0, rowType ) );
            }

            if ( isAddFields ) {
                for ( RelDataTypeField field : rowType.getFieldList() ) {
                    if ( !names.contains( field.getName() ) ) {
                        names.add( field.getName() );
                        values.add( RexInputRef.of( field.getIndex(), rowType ) );
                    }
                }
            }

            // the _data field does not longer exist, as we made a projection "out" of it
            this._dataExists = false;

            return LogicalProject.create( node, values, names );
        }
        return node;
    }


    private RelDataTypeField getTypeFieldOrDefault( RelDataType rowType, String name ) {
        RelDataTypeField field;
        if ( rowType.getFieldNames().contains( name ) ) {
            field = rowType.getField( name, false, false );
        } else {
            field = getDefaultDataField( rowType );
        }
        return field;
    }


}
