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

package org.polypheny.db.languages.mql2alg;

import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Calendar;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Objects;
import java.util.Optional;
import java.util.function.BiFunction;
import java.util.stream.Collectors;
import org.bson.BsonArray;
import org.bson.BsonBoolean;
import org.bson.BsonDocument;
import org.bson.BsonInt32;
import org.bson.BsonNumber;
import org.bson.BsonRegularExpression;
import org.bson.BsonString;
import org.bson.BsonValue;
import org.jetbrains.annotations.Nullable;
import org.polypheny.db.algebra.AlgCollation;
import org.polypheny.db.algebra.AlgCollations;
import org.polypheny.db.algebra.AlgFieldCollation;
import org.polypheny.db.algebra.AlgFieldCollation.Direction;
import org.polypheny.db.algebra.AlgNode;
import org.polypheny.db.algebra.AlgRoot;
import org.polypheny.db.algebra.constant.Kind;
import org.polypheny.db.algebra.core.CorrelationId;
import org.polypheny.db.algebra.core.LaxAggregateCall;
import org.polypheny.db.algebra.core.Values;
import org.polypheny.db.algebra.core.common.Modify;
import org.polypheny.db.algebra.core.common.Modify.Operation;
import org.polypheny.db.algebra.core.document.DocumentProject;
import org.polypheny.db.algebra.fun.AggFunction;
import org.polypheny.db.algebra.logical.document.LogicalDocumentAggregate;
import org.polypheny.db.algebra.logical.document.LogicalDocumentFilter;
import org.polypheny.db.algebra.logical.document.LogicalDocumentModify;
import org.polypheny.db.algebra.logical.document.LogicalDocumentProject;
import org.polypheny.db.algebra.logical.document.LogicalDocumentScan;
import org.polypheny.db.algebra.logical.document.LogicalDocumentSort;
import org.polypheny.db.algebra.logical.document.LogicalDocumentUnwind;
import org.polypheny.db.algebra.logical.document.LogicalDocumentValues;
import org.polypheny.db.algebra.operators.OperatorName;
import org.polypheny.db.algebra.type.AlgDataType;
import org.polypheny.db.algebra.type.AlgDataTypeField;
import org.polypheny.db.algebra.type.DocumentType;
import org.polypheny.db.catalog.entity.Entity;
import org.polypheny.db.catalog.entity.logical.LogicalCollection;
import org.polypheny.db.catalog.entity.logical.LogicalGraph;
import org.polypheny.db.catalog.entity.logical.LogicalGraph.SubstitutionGraph;
import org.polypheny.db.catalog.entity.logical.LogicalNamespace;
import org.polypheny.db.catalog.entity.logical.LogicalTable;
import org.polypheny.db.catalog.exceptions.GenericRuntimeException;
import org.polypheny.db.catalog.snapshot.Snapshot;
import org.polypheny.db.languages.OperatorRegistry;
import org.polypheny.db.languages.QueryLanguage;
import org.polypheny.db.languages.mql.Mql.Type;
import org.polypheny.db.languages.mql.MqlAggregate;
import org.polypheny.db.languages.mql.MqlCollectionStatement;
import org.polypheny.db.languages.mql.MqlCount;
import org.polypheny.db.languages.mql.MqlDelete;
import org.polypheny.db.languages.mql.MqlFind;
import org.polypheny.db.languages.mql.MqlInsert;
import org.polypheny.db.languages.mql.MqlQueryStatement;
import org.polypheny.db.languages.mql.MqlUpdate;
import org.polypheny.db.nodes.Operator;
import org.polypheny.db.plan.AlgCluster;
import org.polypheny.db.processing.QueryContext.ParsedQueryContext;
import org.polypheny.db.rex.RexBuilder;
import org.polypheny.db.rex.RexCall;
import org.polypheny.db.rex.RexElementRef;
import org.polypheny.db.rex.RexIndexRef;
import org.polypheny.db.rex.RexLiteral;
import org.polypheny.db.rex.RexNameRef;
import org.polypheny.db.rex.RexNode;
import org.polypheny.db.schema.document.DocumentUtil;
import org.polypheny.db.schema.document.DocumentUtil.UpdateOperation;
import org.polypheny.db.type.PolyType;
import org.polypheny.db.type.entity.PolyBoolean;
import org.polypheny.db.type.entity.PolyList;
import org.polypheny.db.type.entity.PolyString;
import org.polypheny.db.type.entity.PolyValue;
import org.polypheny.db.type.entity.document.PolyDocument;
import org.polypheny.db.type.entity.numerical.PolyBigDecimal;
import org.polypheny.db.type.entity.numerical.PolyDouble;
import org.polypheny.db.type.entity.numerical.PolyInteger;
import org.polypheny.db.util.BsonUtil;
import org.polypheny.db.util.DateString;
import org.polypheny.db.util.Pair;
import org.polypheny.db.util.TimestampString;


/**
 * Converter class, which transforms a MongoQL command in its MqlNode form to an equal AlgNode
 */
public class MqlToAlgConverter {

    private final Snapshot snapshot;
    private final AlgCluster cluster;
    private RexBuilder builder;
    public static final String MONGO = "mongo";
    private final static Map<String, Operator> mappings = new HashMap<>() {{
        put( "$lt", OperatorRegistry.get( QueryLanguage.from( MONGO ), OperatorName.MQL_LT ) );
        put( "$gt", OperatorRegistry.get( QueryLanguage.from( MONGO ), OperatorName.MQL_GT ) );
        put( "$lte", OperatorRegistry.get( QueryLanguage.from( MONGO ), OperatorName.MQL_LTE ) );
        put( "$gte", OperatorRegistry.get( QueryLanguage.from( MONGO ), OperatorName.MQL_GTE ) );
        put( "$eq", OperatorRegistry.get( QueryLanguage.from( MONGO ), OperatorName.MQL_EQUALS ) );
        put( "$ne", OperatorRegistry.get( OperatorName.NOT_EQUALS ) );
        put( "$in", OperatorRegistry.get( OperatorName.IN ) );
        put( "$nin", OperatorRegistry.get( OperatorName.NOT_IN ) );
        put( "$exists", OperatorRegistry.get( OperatorName.EXISTS ) );
    }};
    private final static List<String> operators = new ArrayList<>();
    private final static Map<String, List<Operator>> gates = new HashMap<>() {{
        put( "$and", Collections.singletonList( OperatorRegistry.get( OperatorName.AND ) ) );
        put( "$or", Collections.singletonList( OperatorRegistry.get( OperatorName.OR ) ) );
        put( "$nor", Arrays.asList( OperatorRegistry.get( OperatorName.AND ), OperatorRegistry.get( OperatorName.NOT ) ) );
        put( "$not", Collections.singletonList( OperatorRegistry.get( OperatorName.NOT ) ) );
    }};
    private final static Map<String, Operator> mathOperators = new HashMap<>() {{
        put( "$subtract", OperatorRegistry.get( QueryLanguage.from( "mongo" ), OperatorName.MINUS ) );
        put( "$add", OperatorRegistry.get( QueryLanguage.from( "mongo" ), OperatorName.PLUS ) );
        put( "$multiply", OperatorRegistry.get( QueryLanguage.from( "mongo" ), OperatorName.MULTIPLY ) );
        put( "$divide", OperatorRegistry.get( QueryLanguage.from( "mongo" ), OperatorName.DIVIDE ) );
        put( "$mod", OperatorRegistry.get( QueryLanguage.from( "mongo" ), OperatorName.MOD ) );
        put( "$pow", OperatorRegistry.get( QueryLanguage.from( "mongo" ), OperatorName.POWER ) );
        put( "$sum", OperatorRegistry.get( QueryLanguage.from( "mongo" ), OperatorName.SUM ) );
        put( "$literal", null );
    }};
    private static final Map<String, AggFunction> accumulators = new HashMap<>() {{
        //$addToSet
        put( "$avg", OperatorRegistry.getAgg( OperatorName.AVG ) );
        put( "$count", OperatorRegistry.getAgg( OperatorName.COUNT ) );
        put( "$first", OperatorRegistry.getAgg( OperatorName.FIRST_VALUE ) );
        put( "$last", OperatorRegistry.getAgg( OperatorName.LAST_VALUE ) );
        put( "$max", OperatorRegistry.getAgg( OperatorName.MAX ) );
        //$mergeObjects
        put( "$min", OperatorRegistry.getAgg( OperatorName.MIN ) );
        //$push
        put( "$stdDevPop", OperatorRegistry.getAgg( OperatorName.STDDEV_POP ) );
        put( "$stdDevSamp", OperatorRegistry.getAgg( OperatorName.STDDEV_SAMP ) );
        put( "$sum", OperatorRegistry.getAgg( OperatorName.SUM ) );
    }};
    private final AlgDataType any;
    private final AlgDataType nullableAny;

    private final AlgDataType jsonType;


    private static final Map<String, Operator> singleMathOperators = new HashMap<>() {{
        put( "$abs", OperatorRegistry.get( OperatorName.ABS ) );
        put( "$acos", OperatorRegistry.get( OperatorName.ACOS ) );
        //singleMathOperators.put( "$acosh", StdOperatorRegistry.get( OperatorName.ACOSH ) );
        put( "$asin", OperatorRegistry.get( OperatorName.ASIN ) );
        put( "$atan", OperatorRegistry.get( OperatorName.ATAN ) );
        put( "$atan2", OperatorRegistry.get( OperatorName.ATAN2 ) );
        //singleMathOperators.put( "$atanh", StdOperatorRegistry.get( OperatorName.ATANH ) );
        put( "$ceil", OperatorRegistry.get( OperatorName.CEIL ) );
        put( "$cos", OperatorRegistry.get( OperatorName.COS ) );
        //singleMathOperators.put( "$cosh", StdOperatorRegistry.get( OperatorName.COSH ) );
        put( "$degreesToRadians", OperatorRegistry.get( OperatorName.DEGREES ) );
        put( "$floor", OperatorRegistry.get( OperatorName.FLOOR ) );
        put( "$ln", OperatorRegistry.get( OperatorName.LN ) );
        put( "$log", OperatorRegistry.get( OperatorName.LN ) );
        put( "$log10", OperatorRegistry.get( OperatorName.LOG10 ) );
        put( "$sin", OperatorRegistry.get( OperatorName.SIN ) );
        //singleMathOperators.put( "$sinh", StdOperatorRegistry.get( OperatorName.SINH ) );
        put( "$sqrt", OperatorRegistry.get( OperatorName.SQRT ) );
        put( "$tan", OperatorRegistry.get( OperatorName.TAN ) );
        //singleMathOperators.put( "$tanh", StdOperatorRegistry.get( OperatorName.TANH ) );
    }};


    static {
        operators.addAll( mappings.keySet() );
        operators.addAll( gates.keySet() );
        operators.addAll( mathOperators.keySet() );
        operators.addAll( singleMathOperators.keySet() );

        // special cases
        operators.add( "$type" );
        operators.add( "$expr" );
        operators.add( "$jsonSchema" );
        operators.add( "$all" );
        operators.add( "$elemMatch" );
        operators.add( "$size" );
    }


    private Optional<RexNode> subElement = Optional.empty();
    private long namespaceId;
    private boolean notActive = false;
    private boolean usesDocumentModel;
    private Entity entity;


    public MqlToAlgConverter( Snapshot snapshot, AlgCluster cluster ) {
        this.snapshot = snapshot;
        this.cluster = Objects.requireNonNull( cluster );
        this.any = this.cluster.getTypeFactory().createPolyType( PolyType.ANY );
        this.nullableAny = this.cluster.getTypeFactory().createTypeWithNullability( any, true );

        this.jsonType = this.cluster.getTypeFactory().createPolyType( PolyType.JSON );

        resetDefaults();
    }


    /**
     * This class is reused and has to reset these values each time this happens
     */
    private void resetDefaults() {
        notActive = false;
        subElement = Optional.empty();
        entity = null;
    }


    public AlgRoot convert( ParsedQueryContext context ) {
        resetDefaults();
        this.namespaceId = context.getNamespaceId();
        if ( context.getQueryNode().orElseThrow() instanceof MqlCollectionStatement ) {
            return convert( (MqlCollectionStatement) context.getQueryNode().orElseThrow() );
        }
        throw new GenericRuntimeException( "DML or DQL need a collection" );
    }


    /**
     * Converts the initial MongoQl by stepping through it iteratively
     *
     * @param query the query in MqlNode format
     * @return the {@link AlgNode} format of the initial query
     */
    public AlgRoot convert( MqlCollectionStatement query ) {
        Type kind = query.getMqlKind();
        this.entity = getEntity( query, namespaceId );

        AlgNode node = LogicalDocumentScan.create( cluster, entity );
        this.usesDocumentModel = true;

        AlgDataType rowType = entity.getTupleType();

        this.builder = new RexBuilder( cluster.getTypeFactory() );

        return switch ( kind ) {
            case FIND -> {
                AlgNode find = convertFind( (MqlFind) query, rowType, node );
                yield AlgRoot.of( find, find.getTupleType(), Kind.SELECT );
            }
            case COUNT -> {
                AlgNode count = convertCount( (MqlCount) query, rowType, node );
                yield AlgRoot.of( count, count.getTupleType(), Kind.SELECT );
            }
            case AGGREGATE -> {
                AlgNode aggregate = convertAggregate( (MqlAggregate) query, rowType, node );
                yield AlgRoot.of( aggregate, Kind.SELECT );
            }
            /// dmls
            case INSERT -> AlgRoot.of( convertInsert( (MqlInsert) query, entity ), Kind.INSERT );
            case DELETE, FIND_DELETE -> AlgRoot.of( convertDelete( (MqlDelete) query, entity, node ), Kind.DELETE );
            case UPDATE -> AlgRoot.of( convertUpdate( (MqlUpdate) query, entity, node ), Kind.UPDATE );
            default -> throw new IllegalStateException( "Unexpected value: " + kind );
        };
    }


    private Entity getEntity( MqlCollectionStatement query, long namespaceId ) {
        LogicalNamespace namespace = snapshot.getNamespace( namespaceId ).orElseThrow();

        Optional<LogicalCollection> optionalDoc = snapshot.doc().getCollection( namespace.id, query.getCollection() );
        if ( optionalDoc.isPresent() ) {
            return optionalDoc.get();
        }

        Optional<LogicalTable> optionalRel = snapshot.rel().getTable( namespace.id, query.getCollection() );
        if ( optionalRel.isPresent() ) {
            return optionalRel.get();
        }

        Optional<LogicalGraph> optionalGraph = snapshot.graph().getGraph( namespace.id );
        if ( optionalGraph.isPresent() ) {
            LogicalGraph graph = optionalGraph.get();
            return new SubstitutionGraph( graph.id, query.getCollection(), false, graph.caseSensitive, List.of( PolyString.of( query.getCollection() ) ) );
        }

        throw new GenericRuntimeException( "The used entity does not exist." );
    }


    /**
     * Starts converting a db.collection.update();
     */
    private AlgNode convertUpdate( MqlUpdate query, Entity entity, AlgNode node ) {
        if ( !query.getQuery().isEmpty() ) {
            node = convertQuery( query, entity.getTupleType(), node );
            if ( query.isOnlyOne() ) {
                node = wrapLimit( node, 1 );
            }
        }
        if ( query.isUsesPipeline() ) {
            node = convertReducedPipeline( query, entity.getTupleType(), node, entity );
        } else {
            node = translateUpdate( query, entity.getTupleType(), node, entity );
        }

        return node;
    }


    /**
     * Translates an update document
     * <p>
     * this method is implemented like the reduced update pipeline,
     * but in fact could be combined and therefore optimized a lot more
     */
    private AlgNode translateUpdate( MqlUpdate query, AlgDataType rowType, AlgNode node, Entity entity ) {
        Map<String, List<RexNode>> updates = new HashMap<>();
        Map<String, RexNode> removes = new HashMap<>();
        Map<String, String> renames = new HashMap<>();

        UpdateOperation updateOp;
        for ( Entry<String, BsonValue> entry : query.getUpdate().asDocument().entrySet() ) {
            String op = entry.getKey();
            if ( !entry.getValue().isDocument() ) {
                throw new GenericRuntimeException( "After a update statement a document is needed" );
            }
            Map<String, RexNode> temp = new HashMap<>();
            switch ( op ) {
                case ("$currentDate"):
                    temp = translateCurrentDate( entry.getValue().asDocument() );
                    updateOp = UpdateOperation.REPLACE;
                    break;
                case "$inc":
                    temp = translateInc( entry.getValue().asDocument(), rowType );
                    updateOp = UpdateOperation.REPLACE;
                    break;
                case "$min":
                    temp = translateMinMaxMul( entry.getValue().asDocument(), rowType, OperatorRegistry.get( QueryLanguage.from( MONGO ), OperatorName.MQL_UPDATE_MIN ) );
                    updateOp = UpdateOperation.REPLACE;
                    break;
                case "$max":
                    temp = translateMinMaxMul( entry.getValue().asDocument(), rowType, OperatorRegistry.get( QueryLanguage.from( MONGO ), OperatorName.MQL_UPDATE_MAX ) );
                    updateOp = UpdateOperation.REPLACE;
                    break;
                case "$mul":
                    temp = translateMinMaxMul( entry.getValue().asDocument(), rowType, OperatorRegistry.get( OperatorName.MULTIPLY ) );
                    updateOp = UpdateOperation.REPLACE;
                    break;
                case "$rename":
                    renames.putAll( translateRename( entry.getValue().asDocument() ) );
                    updateOp = UpdateOperation.RENAME;
                    break;
                case "$set":
                    temp = translateSet( entry.getValue().asDocument(), rowType );
                    updateOp = UpdateOperation.REPLACE;
                    break;
                /*case ("$setOnInsert"):
                    updates.putAll( translateSet(  ) );*/
                case "$unset":
                    temp = translateUnset( entry.getValue().asDocument() );
                    updateOp = UpdateOperation.REMOVE;
                    break;
                case "$addToSet":
                    temp = translateAddToSet( entry.getValue().asDocument(), rowType );
                    updateOp = UpdateOperation.REPLACE;
                    break;
                /*case "$pop":
                case "$pull":
                case "$push":
                case "$pullAll":
                case "$each": UNSUPPORTED
                case "$position":
                case "$slice":
                case "$sort":*/
                default:
                    throw new GenericRuntimeException( "The update operation is not supported." );
            }
            if ( query.isOnlyOne() ) {
                node = wrapLimit( node, 1 );
            }
            if ( updateOp == UpdateOperation.REPLACE ) {
                for ( Entry<String, RexNode> update : temp.entrySet() ) {
                    if ( !updates.containsKey( update.getKey() ) ) {
                        updates.put( update.getKey(), new ArrayList<>() );
                    }
                    updates.get( update.getKey() ).add( update.getValue() );
                }

            } else if ( updateOp == UpdateOperation.REMOVE ) {
                // value can only be removed once
                removes.putAll( temp );
            }
            // rename was already added

        }

        Map<String, RexNode> groupedUpdates = updates.entrySet().stream().collect( Collectors.toMap( Entry::getKey, e -> e.getValue().size() == 1 ? e.getValue().get( 0 ) : and( e.getValue() ) ) );

        return LogicalDocumentModify.create(
                entity,
                node,
                Operation.UPDATE,
                groupedUpdates,
                List.copyOf( removes.keySet() ),
                renames );//finalizeUpdates( "d", mergedUpdates, rowType, node, entity );


    }


    private RexNode and( List<RexNode> nodes ) {
        return cluster.getRexBuilder().makeCall( OperatorRegistry.get( OperatorName.AND ), nodes );
    }


    /**
     * Update can contain REMOVE, RENAME, REPLACE parts and are combine into a single UPDATE RexCall in this method
     */
    private void mergeUpdates( Map<UpdateOperation, List<Pair<String, RexNode>>> mergedUpdates, AlgDataType rowType, Map<String, RexNode> updates, UpdateOperation updateOp ) {
        List<String> names = rowType.getFieldNames();

        Map<String, Map<String, RexNode>> childUpdates = new HashMap<>();
        Map<String, RexNode> directUpdates = new HashMap<>();

        for ( Entry<String, RexNode> nodeEntry : updates.entrySet() ) {
            String[] splits = nodeEntry.getKey().split( "\\." );
            String parent = splits[0];
            if ( names.contains( nodeEntry.getKey() ) ) {
                // direct update to a field
                directUpdates.put( nodeEntry.getKey(), nodeEntry.getValue() );
                if ( updateOp == UpdateOperation.RENAME ) {
                    throw new GenericRuntimeException( "You cannot rename a fixed field in an update, as this is a ddl" );
                }
            } else {
                String childName = null;
                if ( names.contains( parent ) ) {
                    List<String> childNames = Arrays.asList( splits );
                    childNames.remove( 0 );
                    childName = String.join( ".", childNames );
                }

                if ( childName == null ) {
                    throw new GenericRuntimeException( "the specified field in the update was not found" );
                }

                if ( childUpdates.containsKey( parent ) ) {
                    childUpdates.get( parent ).put( childName, nodeEntry.getValue() );
                } else {
                    Map<String, RexNode> up = new HashMap<>();
                    up.put( childName, nodeEntry.getValue() );
                    childUpdates.put( parent, up );
                }
            }
        }

        if ( !Collections.disjoint( directUpdates.entrySet(), childUpdates.keySet() ) && directUpdates.isEmpty() ) {
            throw new GenericRuntimeException( "DML of a field and its subfields at the same time is not possible" );
        }

        combineUpdate( mergedUpdates, updateOp, childUpdates );
    }


    private void combineUpdate( Map<UpdateOperation, List<Pair<String, RexNode>>> mergedUpdates, UpdateOperation updateOp, Map<String, Map<String, RexNode>> childUpdates ) {
        for ( Entry<String, Map<String, RexNode>> entry : childUpdates.entrySet() ) {
            mergedUpdates.get( updateOp )
                    .addAll(
                            entry.getValue()
                                    .entrySet()
                                    .stream()
                                    .map( k -> new Pair<>( k.getKey(), k.getValue() ) )
                                    .toList() );
        }

    }


    /**
     * Starts translating db.collection.update({"$addToSet": 3})
     * this operation adds a key to an array/list
     */
    private Map<String, RexNode> translateAddToSet( BsonDocument doc, AlgDataType rowType ) {
        Map<String, RexNode> updates = new HashMap<>();
        for ( Entry<String, BsonValue> entry : doc.entrySet() ) {
            RexNode id = getIdentifier( entry.getKey(), rowType );
            RexNode value = convertLiteral( entry.getValue() );
            RexCall addToSet = new RexCall( jsonType, OperatorRegistry.get( QueryLanguage.from( MONGO ), OperatorName.MQL_UPDATE_ADD_TO_SET ), Arrays.asList( id, value ) );

            updates.put( entry.getKey(), addToSet );
        }
        return updates;
    }


    /**
     * Start translation of db.collection.update({$unset: "key"})
     * this excludes the defined key from the document
     */
    private Map<String, RexNode> translateUnset( BsonDocument doc ) {
        Map<String, RexNode> updates = new HashMap<>();
        for ( Entry<String, BsonValue> entry : doc.entrySet() ) {
            updates.put( entry.getKey(), null );
        }
        return updates;
    }


    /**
     * Start translation of db.collection.update({$set: {"key":3}})
     * this adds a field with key "key" and value to the document
     */
    private Map<String, RexNode> translateSet( BsonDocument doc, AlgDataType rowType ) {
        Map<String, RexNode> updates = new HashMap<>();
        for ( Entry<String, BsonValue> entry : doc.entrySet() ) {
            if ( entry.getValue().isDocument() ) {
                updates.put( entry.getKey(), translateDocument( entry.getValue().asDocument(), rowType, entry.getKey() ) );
            } else if ( entry.getValue().isArray() ) {
                updates.put( entry.getKey(), convertLiteral( entry.getValue() ) );
            } else {
                updates.put( entry.getKey(), convertLiteral( entry.getValue() ) );
            }
        }
        return updates;
    }


    /**
     * Start translation of db.collection.update({$rename: {"key": "newKey"}})
     * renames the specified key, named "key" to "newKey"
     */
    private Map<String, String> translateRename( BsonDocument doc ) {
        Map<String, String> updates = new HashMap<>();
        for ( Entry<String, BsonValue> entry : doc.entrySet() ) {
            updates.put( entry.getKey(), entry.getValue().asString().getValue() );
        }
        return updates;
    }


    /**
     * Start translation of db.collection.update({$min: {"key": 3}}) or db.collection.update({$max: {"key": 3}})
     * this compares the key with the value and replaces it if it matches
     */
    private Map<String, RexNode> translateMinMaxMul( BsonDocument doc, AlgDataType rowType, Operator operator ) {
        Map<String, RexNode> updates = new HashMap<>();
        for ( Entry<String, BsonValue> entry : doc.entrySet() ) {
            RexNode id = getIdentifier( entry.getKey(), rowType, true );
            RexLiteral literal = builder.makeBigintLiteral( entry.getValue().asNumber().decimal128Value().bigDecimalValue() );
            updates.put( entry.getKey(), new RexCall( any, operator, Arrays.asList( id, literal ) ) );
        }
        return updates;
    }


    /**
     * Start translation of db.collection.update({$inc: {"key": 3}})
     * this increases the value of the given key with the provided amount
     */
    private Map<String, RexNode> translateInc( BsonDocument doc, AlgDataType rowType ) {
        Map<String, RexNode> updates = new HashMap<>();
        for ( Entry<String, BsonValue> entry : doc.entrySet() ) {
            RexNode id = getIdentifier( entry.getKey(), rowType, true );
            RexLiteral literal = builder.makeBigintLiteral( entry.getValue().asNumber().decimal128Value().bigDecimalValue() );
            updates.put( entry.getKey(), builder.makeCall( DocumentType.ofDoc(), OperatorRegistry.get( QueryLanguage.from( "mongo" ), OperatorName.PLUS ), id, literal ) );
        }
        return updates;
    }


    /**
     * Start translation of db.collection.update({$currentDate: {"key": {"$type": "timestamp"}}})
     * this replaces the value of the given key, with the current date in timestamp or date format
     */
    private Map<String, RexNode> translateCurrentDate( BsonDocument value ) {
        Map<String, RexNode> updates = new HashMap<>();
        for ( Entry<String, BsonValue> entry : value.entrySet() ) {
            RexLiteral timeOrDate;
            if ( entry.getValue().isBoolean() ) {
                timeOrDate = builder.makeDateLiteral( DateString.fromCalendarFields( Calendar.getInstance() ) );
            } else {
                if ( entry.getValue().asDocument().get( "$type" ).asString().getValue().equals( "timestamp" ) ) {
                    timeOrDate = builder.makeTimestampLiteral( TimestampString.fromCalendarFields( Calendar.getInstance() ), 0 );
                } else {
                    timeOrDate = builder.makeDateLiteral( DateString.fromCalendarFields( Calendar.getInstance() ) );
                }

            }
            updates.put( entry.getKey(), timeOrDate );
        }
        return updates;
    }


    /**
     * Starts translating an update pipeline
     */
    private AlgNode convertReducedPipeline( MqlUpdate query, AlgDataType rowType, AlgNode node, Entity entity ) {
        Map<String, RexNode> updates = new HashMap<>();
        Map<UpdateOperation, List<Pair<String, RexNode>>> mergedUpdates = new HashMap<>();
        mergedUpdates.put( UpdateOperation.REMOVE, new ArrayList<>() );
        mergedUpdates.put( UpdateOperation.RENAME, new ArrayList<>() );
        mergedUpdates.put( UpdateOperation.REPLACE, new ArrayList<>() );

        UpdateOperation updateOp;
        for ( BsonValue value : query.getPipeline() ) {
            if ( !value.isDocument() || value.asDocument().size() != 1 ) {
                throw new GenericRuntimeException( "Each initial update steps document in the aggregate pipeline can only have one key." );
            }
            String key = value.asDocument().getFirstKey();
            if ( !value.asDocument().get( key ).isDocument() ) {
                throw new GenericRuntimeException( "The update document needs one key and a document." );
            }
            BsonDocument doc = value.asDocument().get( key ).asDocument();
            updateOp = switch ( key ) {
                case "$addFields", "$set" -> {
                    updates.putAll( translateAddToSet( doc, rowType ) );
                    yield UpdateOperation.REPLACE;
                }
                case "$project", "$unset" -> {
                    updates.putAll( translateUnset( doc ) );
                    yield UpdateOperation.REMOVE;
                }
                default -> throw new GenericRuntimeException( "The used statement is not supported in the update aggregation pipeline" );
            };

            mergeUpdates( mergedUpdates, rowType, updates, updateOp );
            updates.clear();

        }

        return LogicalDocumentModify.create( entity, node, Operation.UPDATE, null, null, null );

    }


    /**
     * Translates a delete operation from its MqlNode format to the {@link AlgNode} form
     */
    private AlgNode convertDelete( MqlDelete query, Entity table, AlgNode node ) {
        if ( !query.getQuery().isEmpty() ) {
            node = convertQuery( query, table.getTupleType(), node );
        }
        if ( query.isOnlyOne() ) {
            node = wrapLimit( node, 1 );
        }

        return LogicalDocumentModify.create(
                table,
                node,
                Modify.Operation.DELETE,
                null,
                null,
                null );
    }


    /**
     * Method transforms an insert into the appropriate {@link LogicalDocumentValues}
     *
     * @param query the insert statement as Mql object
     * @param entity the table/collection into which the values are inserted
     * @return the modified AlgNode
     */
    private AlgNode convertInsert( MqlInsert query, Entity entity ) {
        return LogicalDocumentModify.create(
                entity,
                convertMultipleValues( query.getValues() ),
                Modify.Operation.INSERT,
                null,
                null,
                null );
    }


    private AlgNode convertCount( MqlCount query, AlgDataType rowType, AlgNode node ) {
        node = convertQuery( query, rowType, node );

        return LogicalDocumentAggregate.create(
                node,
                null,
                Collections.singletonList(
                        LaxAggregateCall.create( query.isEstimate() ? "estimatedCount" : "count", OperatorRegistry.getAgg( OperatorName.COUNT ), null ) ) );
    }


    /**
     * To correctly represent the values according to the used model they have to be inserted into their {@link Values}
     * representation
     *
     * @param array the values, which are inserted
     * @return the {@link Values} representation of the values
     */
    private AlgNode convertMultipleValues( BsonArray array ) {
        LogicalDocumentValues docs = (LogicalDocumentValues) LogicalDocumentValues.create( cluster, array.asArray().stream().map( a -> BsonUtil.toPolyValue( a ).asDocument() ).toList() );
        if ( usesDocumentModel ) {
            return docs;
        } else {
            return docs.getRelationalEquivalent();
        }

    }


    /**
     * Starts converting of aggregation pipeline
     * <p>
     * Example:
     * <pre>
     * db.collection.aggregate([
     *      {"$project": {"key":1}}, // {@code ->} {@link #combineProjection(BsonValue, AlgNode, AlgDataType, boolean, boolean)}
     *      {"$match": {"key.subkey": "test"}} // {@code ->} {@link #combineFilter(BsonDocument, AlgNode, AlgDataType)}
     * ])
     * </pre>
     */
    private AlgNode convertAggregate( MqlAggregate query, AlgDataType rowType, AlgNode node ) {

        for ( BsonValue value : query.getPipeline() ) {
            if ( !value.isDocument() && ((BsonDocument) value).size() > 1 ) {
                throw new GenericRuntimeException( "The aggregation pipeline is not used correctly." );
            }
            switch ( ((BsonDocument) value).getFirstKey() ) {
                case "$match":
                    node = combineFilter( value.asDocument().getDocument( "$match" ), node, rowType );
                    node.getTupleType();
                    break;
                case "$unset":
                    node = combineProjection( value.asDocument().get( "$unset" ), node, rowType, false, true );
                    break;
                case "$project":
                    node = combineProjection( value.asDocument().getDocument( "$project" ), node, rowType, false, false );
                    break;
                case "$set":
                case "$addFields":
                    node = combineProjection( value.asDocument().getDocument( ((BsonDocument) value).getFirstKey() ), node, rowType, true, false );
                    break;
                case "$count":
                    node = combineCount( value.asDocument().get( "$count" ), node );
                    break;
                case "$group":
                    node = combineGroup( value.asDocument().get( "$group" ), node, rowType );
                    node.getTupleType();
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
                case "$unwind":
                    node = combineUnwind( value.asDocument().get( "$unwind" ), node );
                    break;
                case "$replaceRoot":
                    node = combineReplaceRoot( value.asDocument().get( "$replaceRoot" ), node, false );
                    break;
                case "$replaceWith":
                    node = combineReplaceRoot( value.asDocument().get( "$replaceWith" ), node, true );
                    break;
                // todo dl add more pipeline statements
                default:
                    throw new IllegalStateException( "Unexpected value: " + ((BsonDocument) value).getFirstKey() );
            }
            if ( rowType != null ) {
                rowType = node.getTupleType();
            }
        }

        return node;
    }


    /**
     * Translates the $replaceRoot or $replaceWith stage of the aggregation pipeline
     *
     * @param value the untranslated operation as BSON format
     * @param node the node, to which the operation is applied
     * @param isWith if the operation is a $replaceWith
     * @return the document with the replaced root
     */
    private AlgNode combineReplaceRoot( BsonValue value, AlgNode node, boolean isWith ) {
        BsonValue newRoot = value;
        if ( !isWith ) {
            if ( !value.isDocument() ) {
                throw new GenericRuntimeException( "$replaceRoot requires a document." );
            }
            BsonDocument doc = value.asDocument();
            if ( !doc.containsKey( "newRoot" ) ) {
                throw new GenericRuntimeException( "$replaceRoot requires a document with the key 'newRoot'" );
            }
            newRoot = doc.get( "newRoot" );
        }

        if ( !newRoot.isDocument() && !newRoot.isString() ) {
            throw new GenericRuntimeException( "The used root for $replaceRoot needs to either be a string or a document" );
        }

        DocumentProject project;
        if ( newRoot.isDocument() ) {
            project = LogicalDocumentProject.create(
                    node,
                    Collections.singletonList( translateDocument( newRoot.asDocument(), node.getTupleType(), null ) ),
                    Collections.singletonList( DocumentType.DOCUMENT_DATA )
            );
        } else {
            if ( !newRoot.asString().getValue().startsWith( "$" ) ) {
                throw new GenericRuntimeException( "The used root needs to be a reference to a field" );
            }

            BsonValue finalNewRoot = newRoot;
            Map<String, RexNode> nodes = new HashMap<>() {{
                put( null, new RexCall( any, OperatorRegistry.get( QueryLanguage.from( MONGO ), OperatorName.MQL_REPLACE_ROOT ), List.of( getIdentifier( finalNewRoot.asString().getValue().substring( 1 ), node.getTupleType() ) ) ) );
            }};

            project = LogicalDocumentProject.create(
                    node,
                    nodes,
                    List.of()
            );
        }
        return project;
    }


    /**
     * Transforms the $unwind stage in the aggregation pipeline
     * this operation unfolds a specified array into multiple records
     * <p>
     * {"test","key",[3,1,"te"]} {@literal ->} {"test","key",3},{"test","key",1},{"test","key","te"}
     *
     * @param value the unparsed $unwind operation
     */
    private AlgNode combineUnwind( BsonValue value, AlgNode node ) {
        if ( !value.isString() && !value.isDocument() ) {
            throw new GenericRuntimeException( "$unwind pipeline stage needs either a document or a string describing the path." );
        }

        String path;
        if ( value.isString() ) {
            path = value.asString().getValue();
        } else {
            if ( !value.asDocument().containsKey( "path" ) && value.asDocument().get( "path" ).isString() ) {
                throw new GenericRuntimeException( "The used document in the $unwind stage needs the key \"path\" with a string value." );
            }
            path = value.asDocument().get( "path" ).asString().getValue();
        }
        if ( !path.startsWith( "$" ) ) {
            throw new GenericRuntimeException( "$unwind pipeline stage needs either a document or a string describing the path, which is prefixed with \"$\"." );
        }
        path = path.substring( 1 );

        return LogicalDocumentUnwind.create( path, node );
    }


    /**
     * Translates a $skip stage of the aggregation pipeline
     * {@code <pre>
     *     { $skip: <positive 64-bit integer> }
     * </pre>}
     *
     * @param value the untransformed BSON value
     * @param node the node up to this point
     * @return the provided node with the applied skip stage
     */
    private AlgNode combineSkip( BsonValue value, AlgNode node ) {
        if ( !value.isNumber() || value.asNumber().intValue() < 0 ) {
            throw new GenericRuntimeException( "$skip pipeline stage needs a positive number after" );
        }

        return LogicalDocumentSort.create( node, AlgCollations.of(), List.of(), convertLiteral( value ), null );
    }


    /**
     * Translates a $skip stage of the aggregation pipeline
     * {@code <pre>
     *     { $limit: <positive 64-bit integer> }
     * </pre>}
     *
     * @param value the untransformed BSON value
     * @param node the node up to this point
     * @return the provided node with the applied limit stage
     */
    private AlgNode combineLimit( BsonValue value, AlgNode node ) {
        if ( !value.isNumber() || value.asNumber().intValue() < 0 ) {
            throw new GenericRuntimeException( "$limit pipeline stage needs a positive number after" );
        }

        return LogicalDocumentSort.create( node, AlgCollations.of(), List.of(), null, convertLiteral( value ) );
    }


    /**
     * Translates a $sort stage of the aggregation pipeline
     * <pre>
     *     { $sort: { <field1>: <sort order>, <field2>: <sort order> ... } }
     * </pre>
     *
     * @param value the untransformed BSON value
     * @param node the node up to this point
     * @param rowType the rowType of the relnode, which is sorted
     * @return the provided node with the applied sort stage
     */
    private AlgNode combineSort( BsonValue value, AlgNode node, AlgDataType rowType ) {
        if ( !value.isDocument() ) {
            throw new GenericRuntimeException( "$sort pipeline stage needs a document after" );
        }

        List<String> names = new ArrayList<>();
        List<Direction> dirs = new ArrayList<>();

        for ( Entry<String, BsonValue> entry : value.asDocument().entrySet() ) {
            names.add( entry.getKey() );
            if ( entry.getValue().asNumber().intValue() == 1 ) {
                dirs.add( Direction.ASCENDING );
            } else {
                dirs.add( Direction.DESCENDING );
            }
        }

        return conditionalWrap(
                node,
                rowType,
                names,
                ( newNode, nodes ) -> LogicalDocumentSort.create( newNode, AlgCollations.of( generateCollation( dirs, names, names ) ), nodes, null, null ) );
    }


    /**
     * This function wraps document fields which are either hidden in the default data field _data or in another parent field
     */
    private AlgNode conditionalWrap( AlgNode node, AlgDataType rowType, List<String> names, BiFunction<AlgNode, List<RexNode>, AlgNode> nodeFunction ) {
        List<RexNode> projectionNodes = new ArrayList<>();
        for ( String name : names ) {
            RexNode identifier = getIdentifier( name, rowType );
            projectionNodes.add( identifier );
        }
        return nodeFunction.apply( node, projectionNodes );
    }


    private List<AlgFieldCollation> generateCollation( List<Direction> dirs, List<String> names, List<String> rowNames ) {
        List<AlgFieldCollation> collations = new ArrayList<>();
        int pos = 0;
        int index;
        for ( String name : names ) {
            index = rowNames.indexOf( name );
            collations.add( new AlgFieldCollation( index, dirs.get( pos ) ) );
            pos++;
        }
        return collations;
    }


    /**
     * Translates a $group stage of the aggregation pipeline
     * <pre>
     * {
     *   $group:
     *     {
     *       _id: <expression>, // Group By Expression
     *       <field1>: { <accumulator1> : <expression1> },
     *       ...
     *     }
     *  }
     * </pre>
     *
     * @param value the untransformed BSON value
     * @param node the node up to this point
     * @param rowType the rowType of the relnode, which is grouped
     * @return the provided node with the applied group stage
     */
    private AlgNode combineGroup( BsonValue value, AlgNode node, AlgDataType rowType ) {
        if ( !value.isDocument() || !value.asDocument().containsKey( "_id" ) ) {
            throw new GenericRuntimeException( "$group pipeline stage needs a document after, which defines a _id" );
        }

        Map<String, AggFunction> nameOps = new HashMap<>();

        Map<String, RexNode> nameNodes = new HashMap<>();


        for ( Entry<String, BsonValue> entry : value.asDocument().entrySet() ) {
            if ( entry.getKey().equals( "_id" ) ) {
                if ( entry.getValue().isNull() ) {
                    nameNodes.put( "_id", new RexLiteral( null, nullableAny, PolyType.NULL ) );
                } else if ( entry.getValue().isString() ) {
                    nameNodes.put( "_id", getIdentifier( entry.getValue().asString().getValue().substring( 1 ), rowType ) );
                } else if ( entry.getValue().isDocument() ) {
                    for ( Entry<String, BsonValue> idEntry : entry.getValue().asDocument().entrySet() ) {
                        nameNodes.put( idEntry.getValue().asString().getValue(), getIdentifier( idEntry.getValue().asString().getValue().substring( 1 ), rowType ) );
                    }
                } else {
                    throw new GenericRuntimeException( "$group takes as _id values either a document or a string" );
                }
            } else {
                if ( !entry.getValue().isDocument() ) {
                    throw new GenericRuntimeException( "$group needs a document with an accumulator and an expression" );
                }
                BsonDocument doc = entry.getValue().asDocument();
                nameOps.put( entry.getKey(), accumulators.get( doc.getFirstKey() ) );

                AlgDataType nullableDouble = cluster.getTypeFactory().createTypeWithNullability( cluster.getTypeFactory().createPolyType( PolyType.DOUBLE ), true );
                // when using aggregations MongoQl automatically casts to doubles
                String key = doc.get( doc.getFirstKey() ).asString().getValue().substring( 1 );

                nameNodes.put( entry.getKey(), new RexNameRef( key, null, nullableDouble ) );
            }
        }

        return groupBy( value, nameNodes, node, node.getTupleType(), nameOps );
    }


    private AlgNode groupBy( BsonValue value, Map<String, RexNode> nameNodes, AlgNode node, AlgDataType rowType, Map<String, AggFunction> nameOps ) {
        BsonValue groupBy = value.asDocument().get( "_id" );

        List<LaxAggregateCall> convertedAggs = new ArrayList<>();

        for ( Entry<String, AggFunction> agg : nameOps.entrySet() ) {

            convertedAggs.add(
                    LaxAggregateCall.create(
                            agg.getKey(),
                            agg.getValue(),
                            nameNodes.get( agg.getKey() ) ) );
        }

        if ( !groupBy.isNull() ) {
            String groupName = groupBy.asString().getValue().substring( 1 );
            rowType.getFieldNames().indexOf( groupName );

            node = LogicalDocumentAggregate.create(
                    node,
                    new RexNameRef( groupName, null, DocumentType.ofDoc() ),
                    convertedAggs );
        } else {
            node = LogicalDocumentAggregate.create(
                    node,
                    null,
                    convertedAggs );

        }
        return node;
    }


    /**
     * Translates a $count stage of the aggregation pipeline
     * <pre>
     *     { $count: <string> }
     * </pre>
     *
     * @param value the untransformed BSON value
     * @param node the node up to this point
     * @return the provided node with the applied $count stage
     */
    private AlgNode combineCount( BsonValue value, AlgNode node ) {
        if ( !value.isString() ) {
            throw new GenericRuntimeException( "$count pipeline stage needs only a string" );
        }
        return LogicalDocumentAggregate.create(
                node,
                null,
                Collections.singletonList(
                        LaxAggregateCall.create( value.asString().getValue(), OperatorRegistry.getAgg( OperatorName.COUNT ), null ) ) );
    }


    private AlgNode convertFind( MqlFind query, AlgDataType rowType, AlgNode node ) {
        node = convertQuery( query, rowType, node );

        if ( query.getProjection() != null && !query.getProjection().isEmpty() ) {
            node = combineProjection( query.getProjection(), node, rowType, false, false );
        }

        if ( query.isOnlyOne() ) {
            node = wrapLimit( node, 1 );
        }

        if ( query.getLimit() != null ) {
            node = wrapLimit( node, query.getLimit() );
        }

        return node;
    }


    private AlgNode convertQuery( MqlQueryStatement query, AlgDataType rowType, AlgNode node ) {
        if ( query.getQuery() != null && !query.getQuery().isEmpty() ) {
            node = combineFilter( query.getQuery(), node, rowType );
        }
        return node;
    }


    private AlgNode wrapLimit( AlgNode node, int limit ) {
        final AlgCollation collation = cluster.traitSet().canonize( AlgCollations.of( new ArrayList<>() ) );
        return LogicalDocumentSort.create(
                node,
                collation,
                List.of(),
                null,
                new RexLiteral(
                        PolyBigDecimal.of( new BigDecimal( limit ) ),
                        cluster.getTypeFactory()
                                .createPolyType( PolyType.INTEGER ), PolyType.DECIMAL )
        );
    }


    private AlgNode combineFilter( BsonDocument filter, AlgNode node, AlgDataType rowType ) {
        RexNode condition = translateDocument( filter, rowType, null );

        return LogicalDocumentFilter.create( node, condition );
    }


    private static PolyValue getPolyValue( BsonValue value ) {
        switch ( value.getBsonType() ) {
            case DOUBLE:
                return PolyDouble.of( value.asDouble().getValue() );
            case STRING:
                return PolyString.of( value.asString().getValue() );
            case DOCUMENT:
                Map<PolyString, PolyValue> map = new HashMap<>();
                for ( Entry<String, BsonValue> entry : value.asDocument().entrySet() ) {
                    map.put( PolyString.of( entry.getKey() ), getPolyValue( entry.getValue() ) );
                }

                return PolyDocument.ofDocument( map );
            case ARRAY:
                List<PolyValue> list = new ArrayList<>();
                for ( BsonValue bson : value.asArray() ) {
                    list.add( getPolyValue( bson ) );
                }
                return PolyList.of( list );
            case BOOLEAN:
                return new PolyBoolean( value.asBoolean().getValue() );
            case INT32:
                return new PolyInteger( value.asInt32().getValue() );
        }
        throw new GenericRuntimeException( "Not implemented Comparable transform: " + value );
    }


    private RexNode convertEntry( String key, String parentKey, BsonValue bsonValue, AlgDataType rowType ) {
        List<RexNode> operands = new ArrayList<>();
        if ( !key.startsWith( "$" ) ) {
            return convertField( parentKey == null ? key : parentKey + "." + key, bsonValue, rowType );
        } else {
            if ( operators.contains( key ) ) {
                if ( gates.containsKey( key ) ) {
                    return convertGate( key, parentKey, bsonValue, rowType );

                } else if ( singleMathOperators.containsKey( key ) ) {
                    return convertSingleMath( key, bsonValue, rowType );
                } else if ( key.equals( "$mod" ) ) {
                    RexNode id = getIdentifier( parentKey, rowType );
                    return convertMod( id, bsonValue, rowType );
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
                        AlgDataType type = cluster.getTypeFactory().createPolyType( PolyType.BOOLEAN );
                        return new RexCall( type, OperatorRegistry.get( QueryLanguage.from( MONGO ), OperatorName.MQL_EQUALS ), Arrays.asList( id, node ) );
                    } else {
                        return node;
                    }
                } else {
                    return switch ( key ) {
                        case "$exists" -> convertExists( bsonValue, parentKey, rowType );
                        case "$type" -> convertType( bsonValue, parentKey, rowType );
                        case "$expr" -> convertExpr( bsonValue, parentKey, rowType );
                        case "$jsonSchema" ->
                            // jsonSchema is a general match
                                convertJsonSchema( bsonValue, rowType );
                        case "$all" -> convertAll( bsonValue, parentKey, rowType );
                        case "$elemMatch" -> convertElemMatch( bsonValue, parentKey, rowType );
                        case "$size" -> convertSize( bsonValue, parentKey, rowType );
                        default -> translateLogical( key, parentKey, bsonValue, rowType );
                    };
                }
            }  // handle others
        }
        return getFixedCall( operands, OperatorRegistry.get( OperatorName.AND ), PolyType.BOOLEAN );
    }


    private RexNode convertMod( RexNode id, BsonValue bsonValue, AlgDataType rowType ) {
        String msg = "$mod requires an array of two values or documents.";
        if ( !bsonValue.isArray() && bsonValue.asArray().size() != 2 ) {
            throw new GenericRuntimeException( msg );
        }

        BsonValue divider = bsonValue.asArray().get( 0 );
        RexNode rexDiv;
        if ( divider.isDocument() ) {
            rexDiv = translateDocument( divider.asDocument(), rowType, null );
        } else if ( divider.isNumber() ) {
            rexDiv = convertLiteral( divider );
        } else {
            throw new GenericRuntimeException( msg );
        }

        BsonValue remainder = bsonValue.asArray().get( 1 );
        RexNode rexRemainder;
        if ( remainder.isDocument() ) {
            rexRemainder = translateDocument( remainder.asDocument(), rowType, null );
        } else if ( remainder.isNumber() ) {
            rexRemainder = convertLiteral( remainder );
        } else {
            throw new GenericRuntimeException( msg );
        }

        RexNode node = new RexCall(
                this.cluster.getTypeFactory().createPolyType( PolyType.INTEGER ),
                OperatorRegistry.get( OperatorName.MOD ),
                Arrays.asList(
                        this.builder.makeCast( cluster.getTypeFactory().createPolyType( PolyType.INTEGER ), id ),
                        rexDiv ) );

        AlgDataType type = cluster.getTypeFactory().createPolyType( PolyType.BOOLEAN );

        return new RexCall( type, OperatorRegistry.get( QueryLanguage.from( MONGO ), OperatorName.MQL_EQUALS ), Arrays.asList( node, rexRemainder ) );
    }


    /**
     * Converts a $slice projection
     * <pre>
     *     { $slice: [ <array>, <n> ] }
     * </pre>
     * or
     * <pre>
     *     { $slice: [ <array>, <position>, <n> ] }
     * </pre>
     *
     * @param key the key of the parent document
     * @param value the BSON object, which holds the $slice information
     * @param rowType the row information onto which the projection is applied
     * @return the applied node
     */
    private RexNode convertSlice( String key, BsonValue value, AlgDataType rowType ) {
        BsonNumber skip = new BsonInt32( 0 );
        BsonNumber elements;
        RexNode id = getIdentifier( key, rowType );
        if ( value.isNumber() ) {
            elements = value.asInt32();
        } else if ( value.isArray() && value.asArray().size() == 2 ) {
            skip = value.asArray().get( 0 ).asInt32();
            elements = value.asArray().get( 1 ).asInt32();
        } else {
            throw new GenericRuntimeException( "After a $slice projection a number or an array of 2 is needed" );
        }

        return new RexCall( any, OperatorRegistry.get( QueryLanguage.from( MONGO ), OperatorName.MQL_SLICE ), Arrays.asList( id, convertLiteral( skip ), convertLiteral( elements ) ) );
    }


    /**
     * Converts a $arrayElemAt projection
     * <pre>
     *     { $arrayElemAt: [ <array>, <idx> ] }
     * </pre>
     *
     * @param key the key of the parent document
     * @param value the BSON object, which holds the $arrayElemAt information
     * @param rowType the row information onto which the projection is applied
     * @return the applied node
     */
    private RexNode convertArrayAt( String key, BsonValue value, AlgDataType rowType ) {
        String msg = "$arrayElemAt has following structure { $arrayElemAt: [ <array>, <idx> ] }";
        if ( value.isArray() ) {
            List<RexNode> nodes = convertArray( key, (BsonArray) value, true, rowType, msg );
            if ( nodes.size() > 2 ) {
                throw new GenericRuntimeException( msg );
            }
            return new RexCall( any, OperatorRegistry.get( QueryLanguage.from( MONGO ), OperatorName.MQL_ITEM ), Arrays.asList( nodes.get( 0 ), nodes.get( 1 ) ) );

        } else {
            throw new GenericRuntimeException( msg );
        }
    }


    private RexNode convertMath( String key, String parentKey, BsonValue bsonValue, AlgDataType rowType, boolean isExpr ) {
        if ( key.equals( "$literal" ) ) {
            return convertLiteral( bsonValue );
        }
        Operator op;
        if ( !isExpr ) {
            op = mathOperators.get( key );
        } else {
            op = mappings.get( key );
        }

        String errorMsg = "After a " + String.join( ",", mathOperators.keySet() ) + " a list of literals or documents is needed.";
        if ( bsonValue.isArray() ) {
            List<RexNode> nodes = convertArray( parentKey, bsonValue.asArray(), true, rowType, errorMsg );

            return getFixedCall( nodes, op, isExpr ? PolyType.BOOLEAN : PolyType.ANY );
        } else {
            throw new GenericRuntimeException( errorMsg );
        }
    }


    private RexNode convertSingleMath( String key, BsonValue value, AlgDataType rowType ) {
        Operator op = singleMathOperators.get( key );
        if ( value.isArray() ) {
            throw new GenericRuntimeException( "The " + key + " operator needs either a single expression or a document." );
        }
        RexNode node;
        if ( value.isDocument() ) {
            node = translateDocument( value.asDocument(), rowType, null );
        } else if ( value.isString() && value.asString().getValue().startsWith( "$" ) ) {
            node = getIdentifier( value.asString().getValue().substring( 1 ), rowType );
        } else {
            node = convertLiteral( value );
        }

        return new RexCall( any, op, Collections.singletonList( node ) );
    }


    private RexNode convertGate( String key, String parentKey, BsonValue bsonValue, AlgDataType rowType ) {

        Operator op;
        switch ( key ) {
            case "$and":
                if ( notActive ) {
                    throw new GenericRuntimeException( "$logical operations inside $not operations are not possible." );
                }
                op = OperatorRegistry.get( OperatorName.AND );
                return convertLogicalArray( parentKey, bsonValue, rowType, op, false );
            case "$or":
                if ( notActive ) {
                    throw new GenericRuntimeException( "$logical operations inside $not operations are not possible." );
                }
                op = OperatorRegistry.get( OperatorName.OR );
                return convertLogicalArray( parentKey, bsonValue, rowType, op, false );
            case "$nor":
                if ( notActive ) {
                    throw new GenericRuntimeException( "$logical operations inside $not operations are not possible." );
                }
                op = OperatorRegistry.get( OperatorName.AND );
                return convertLogicalArray( parentKey, bsonValue, rowType, op, true );
            case "$not":
                this.notActive = true;
                op = OperatorRegistry.get( OperatorName.NOT );
                if ( bsonValue.isArray() && bsonValue.asArray().size() == 1 ) {
                    AlgDataType type = cluster.getTypeFactory().createPolyType( PolyType.BOOLEAN );
                    return new RexCall( type, op, Collections.singletonList( translateDocument( bsonValue.asArray().get( 0 ).asDocument(), rowType, parentKey ) ) );
                } else {
                    throw new GenericRuntimeException( "After a $not an array is needed" );
                }

            default:
                throw new GenericRuntimeException( "This logical operator was not recognized:" );
        }
    }


    private RexNode convertLogicalArray( String parentKey, BsonValue bsonValue, AlgDataType rowType, Operator op, boolean isNegated ) {
        String errorMsg = "After logical operators \"$and\",\"$or\" and \"nor\" an array of documents is needed";
        if ( bsonValue.isArray() ) {
            List<RexNode> operands = convertArray( parentKey, bsonValue.asArray(), false, rowType, errorMsg );
            if ( isNegated ) {
                operands = operands.stream().map( this::negate ).toList();
            }
            return getFixedCall( operands, op, PolyType.BOOLEAN );
        } else {
            throw new GenericRuntimeException( errorMsg );
        }
    }


    private List<RexNode> convertArray( String parentKey, BsonArray bsonValue, boolean allowsLiteral, AlgDataType rowType, String errorMsg ) {
        List<RexNode> operands = new ArrayList<>();
        for ( BsonValue value : bsonValue ) {
            if ( value.isDocument() ) {
                operands.add( translateDocument( value.asDocument(), rowType, parentKey ) );
            } else if ( value.isString() && value.asString().getValue().startsWith( "$" ) ) {
                operands.add( getIdentifier( value.asString().getValue().substring( 1 ), rowType ) );
            } else if ( allowsLiteral ) {
                operands.add( convertLiteral( value ) );
            } else {
                throw new GenericRuntimeException( errorMsg );
            }
        }
        return operands;
    }


    private RexNode convertField( String parentKey, BsonValue bsonValue, AlgDataType rowType ) {

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
                nodes.add( DocumentUtil.getArray(
                        arr,
                        cluster.getTypeFactory().createArrayType( nullableAny, arr.size() ) ) );
            } else if ( bsonValue.isRegularExpression() ) {
                return convertRegex( bsonValue, parentKey, rowType );
            } else {
                nodes.add( convertLiteral( bsonValue ) );
            }

            return getFixedCall( nodes, OperatorRegistry.get( QueryLanguage.from( MONGO ), OperatorName.MQL_EQUALS ), PolyType.BOOLEAN );
        }
    }


    private RexNode getIdentifier( String parentKey, AlgDataType rowType ) {
        return getIdentifier( parentKey, rowType, false );
    }


    private RexNode getIdentifier( String parentKey, AlgDataType rowType, boolean useAccess ) {
        if ( useAccess ) {
            return attachAccess( parentKey, rowType );
        }
        // as it is possible to query relational schema with mql we have to block this step when this happens
        if ( !usesDocumentModel ) {
            throw new GenericRuntimeException( "The used identifier is not part of the table." );
        }

        // we look if we already extracted a part of the document

        return translateDocValue( null, parentKey );

    }


    private RexNode attachAccess( String parentKey, AlgDataType rowType ) {
        AlgDataTypeField field = rowType.getField( parentKey, false, false );
        return attachAccess( field.getIndex(), rowType );
    }


    private RexNode attachAccess( int index, AlgDataType rowType ) {
        CorrelationId correlId = cluster.createCorrel();
        cluster.getMapCorrelToAlg().put( correlId, LogicalDocumentScan.create( cluster, entity ) );
        return builder.makeFieldAccess( builder.makeCorrel( rowType, correlId ), index );
    }


    private RexNode getFixedCall( List<RexNode> operands, Operator op, PolyType polyType ) {
        if ( operands.size() == 1 ) {
            if ( op.getKind() == Kind.NOT && operands.get( 0 ) instanceof RexCall && ((RexCall) operands.get( 0 )).op.getKind() == Kind.NOT ) {
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
            if ( !toAdd.isEmpty() ) {
                operands.addAll( toAdd );
                operands.removeAll( toRemove );
            }

            return new RexCall( cluster.getTypeFactory().createTypeWithNullability( cluster.getTypeFactory().createPolyType( polyType ), true ), op, operands );
        }
    }


    private RexNode translateDocument( BsonDocument bsonDocument, AlgDataType rowType, String parentKey ) {
        List<RexNode> operands = new ArrayList<>();

        for ( Entry<String, BsonValue> entry : bsonDocument.entrySet() ) {
            if ( entry.getKey().equals( "$regex" ) ) {
                operands.add( convertRegex( bsonDocument, parentKey, rowType ) );
            } else if ( !entry.getKey().equals( "$options" ) ) {
                // normal handling
                operands.add( convertEntry( entry.getKey(), parentKey, entry.getValue(), rowType ) );
            }
        }
        return getFixedCall( operands, OperatorRegistry.get( OperatorName.AND ), PolyType.BOOLEAN );
    }


    private RexNode translateLogical( String key, String parentKey, BsonValue bsonValue, AlgDataType rowType ) {
        Operator op;
        List<RexNode> nodes = new ArrayList<>();
        op = mappings.get( key );
        switch ( op.getKind() ) {
            case IN:
            case NOT_IN:
                return convertIn( bsonValue, op, parentKey, rowType );
            default:
                if ( parentKey != null ) {
                    RexNode id = getIdentifier( parentKey, rowType );
                    nodes.add( id );
                }
                nodes.add( convertLiteral( bsonValue ) );
                return new RexCall( cluster.getTypeFactory().createPolyType( PolyType.BOOLEAN ), op, nodes );
        }
    }


    /**
     * Translates a $regex filter field
     * <pre>
     *      { <field>: { $regex: /pattern/, $options: '<options>' } }
     *      { <field>: { $regex: 'pattern', $options: '<options>' } }
     *      { <field>: { $regex: /pattern/<options> } }
     * </pre>
     *
     * @param bson the regex information as BSON,
     * which is either a document with the necessary keys ($regex, $options) or BsonRegularExpression
     * @param parentKey the key of the parent document
     * @param rowType the rowType of the node which is filtered by regex
     * @return the filtered node
     */
    private RexNode convertRegex( BsonValue bson, String parentKey, AlgDataType rowType ) {
        String options = "";
        BsonValue regex;
        if ( bson.isDocument() ) {
            BsonDocument bsonDocument = bson.asDocument();
            if ( bsonDocument.size() == 2 && bsonDocument.containsKey( "$regex" ) && bsonDocument.containsKey( "$options" ) ) {
                options = bsonDocument.get( "$options" ).isString() ? bsonDocument.get( "$options" ).asString().getValue() : "";
            }
            regex = bsonDocument.get( "$regex" );
        } else if ( bson.isRegularExpression() ) {
            regex = bson;
        } else {
            throw new GenericRuntimeException( "$regex either needs to be either a document or a regular expression." );
        }

        String stringRegex;
        if ( regex.isString() ) {
            stringRegex = regex.asString().getValue();
        } else if ( regex.isRegularExpression() ) {
            BsonRegularExpression regbson = regex.asRegularExpression();
            stringRegex = regbson.getPattern();
            options += regbson.getOptions();
        } else {
            throw new GenericRuntimeException( "$regex needs to be either a regular expression or a string" );
        }

        return getRegex( stringRegex, options, parentKey, rowType );
    }


    private RexCall getRegex( String stringRegex, String options, String parentKey, AlgDataType rowType ) {
        return new RexCall(
                cluster.getTypeFactory().createPolyType( PolyType.BOOLEAN ),
                OperatorRegistry.get( QueryLanguage.from( MONGO ), OperatorName.MQL_REGEX_MATCH ),
                Arrays.asList(
                        getIdentifier( parentKey, rowType ),
                        convertLiteral( new BsonString( stringRegex ) ),
                        convertLiteral( new BsonBoolean( options.contains( "i" ) ) ),
                        convertLiteral( new BsonBoolean( options.contains( "m" ) ) ),
                        convertLiteral( new BsonBoolean( options.contains( "x" ) ) ),
                        convertLiteral( new BsonBoolean( options.contains( "s" ) ) )
                ) );
    }


    /**
     * Converts an $exists field according to the provided information
     *
     * <pre>
     * { field: { $exists: <boolean> } }
     * </pre>
     *
     * @param value the information of the $exists field
     * @param parentKey the name of the parent key
     * @param rowType the row information of the filtered node
     * @return a node with the applied filter
     */
    private RexNode convertExists( BsonValue value, String parentKey, AlgDataType rowType ) {
        if ( !value.isBoolean() ) {
            throw new GenericRuntimeException( "$exists without a boolean is not supported" );
        }

        List<String> keys = Arrays.asList( parentKey.split( "\\." ) );

        return new RexCall(
                cluster.getTypeFactory().createPolyType( PolyType.BOOLEAN ),
                OperatorRegistry.get( QueryLanguage.from( MONGO ), OperatorName.MQL_EXISTS ),
                Arrays.asList( RexIndexRef.of( 0, rowType ), convertLiteral( value.asBoolean() ), getStringArray( keys ) ) );
    }


    private int getIndexOfParentField( String parentKey, AlgDataType rowType ) {
        int index = rowType.getFieldNames().indexOf( parentKey );
        if ( index < 0 ) {
            index = rowType.getFieldNames().indexOf( parentKey.split( "\\." )[0] );
            if ( index < 0 ) {
                throw new GenericRuntimeException( "The field does not exist in the collection" );
            }
        }
        return index;
    }


    private RexNode convertExpr( BsonValue bsonValue, String parentKey, AlgDataType rowType ) {
        if ( bsonValue.isDocument() && bsonValue.asDocument().size() == 1 ) {
            BsonDocument doc = bsonValue.asDocument();
            return convertMath( doc.getFirstKey(), parentKey, doc.get( doc.getFirstKey() ), rowType, true );

        } else {
            throw new GenericRuntimeException( "After $expr there needs to be a document with a single entry" );
        }
    }


    /**
     * Converts a $jsonSchema filter field
     * <pre>
     *     { $jsonSchema: <JSON Schema object> }
     * </pre>
     *
     * @param bsonValue the information of the $jsonSchema in BSON format
     * @param rowType the row information of the filtered node
     * @return a node with the applied filter
     */
    private RexNode convertJsonSchema( BsonValue bsonValue, AlgDataType rowType ) {
        if ( bsonValue.isDocument() ) {
            return new RexCall( nullableAny, OperatorRegistry.get( QueryLanguage.from( MONGO ), OperatorName.MQL_JSON_MATCH ), Collections.singletonList( RexIndexRef.of( getIndexOfParentField( "d", rowType ), rowType ) ) );
        } else {
            throw new GenericRuntimeException( "After $jsonSchema there needs to follow a document" );
        }
    }


    /**
     * Converts a $size filter field
     * <pre>
     *     { $size: <expression> }
     * </pre>
     *
     * @param bsonValue the information of the $size in BSON format
     * @param parentKey the name of the parent key
     * @param rowType the row information of the filtered node
     * @return a node with the applied filter
     */
    private RexNode convertSize( BsonValue bsonValue, String parentKey, AlgDataType rowType ) {
        if ( bsonValue.isNumber() ) {
            RexNode id = getIdentifier( parentKey, rowType );
            return new RexCall( cluster.getTypeFactory().createPolyType( PolyType.BOOLEAN ), OperatorRegistry.get( QueryLanguage.from( MONGO ), OperatorName.MQL_SIZE_MATCH ), Arrays.asList( id, convertLiteral( bsonValue ) ) );
        } else {
            throw new GenericRuntimeException( "After $size there needs to follow a number" );
        }
    }


    /**
     * Converts a $elemMatch field, this only returns if the fields is an array and a value matches the provided queries
     * <pre>
     *     { <field>: { $elemMatch: { <query1>, <query2>, ... } } }
     * </pre>
     *
     * @param bsonValue the information of the $elemMatch in BSON format
     * @param parentKey the name of the parent key
     * @param rowType the row information of the filtered node
     * @return a node with the applied filter
     */
    private RexNode convertElemMatch( BsonValue bsonValue, String parentKey, AlgDataType rowType ) {
        if ( !bsonValue.isDocument() ) {
            throw new GenericRuntimeException( "After $elemMatch there needs to follow a document" );
        }
        this.subElement = Optional.of( new RexElementRef( new RexNameRef( parentKey, null, DocumentType.ofDoc() ), DocumentType.ofDoc() ) );
        List<RexNode> nodes = new ArrayList<>();
        for ( Entry<String, BsonValue> entry : bsonValue.asDocument().entrySet() ) {
            nodes.add( convertEntry( entry.getKey(), "", entry.getValue(), rowType ) );
        }
        this.subElement = Optional.empty();

        RexNode op = getFixedCall( nodes, OperatorRegistry.get( OperatorName.AND ), PolyType.BOOLEAN );

        return new RexCall( cluster.getTypeFactory().createPolyType( PolyType.BOOLEAN ), OperatorRegistry.get( QueryLanguage.from( MONGO ), OperatorName.MQL_ELEM_MATCH ), Arrays.asList( getIdentifier( parentKey, rowType ), op ) );
    }


    /**
     * Converts an $all field, this only returns if the fields is an array and all fields matches the provided queries
     * <pre>
     *     { <field>: { $all: [ <value1> , <value2> ... ] } }
     * </pre>
     *
     * @param bsonValue the information of the $all in BSON format
     * @param parentKey the name of the parent key
     * @param rowType the row information of the filtered node
     * @return a node with the applied filter
     */
    private RexNode convertAll( BsonValue bsonValue, String parentKey, AlgDataType rowType ) {
        AlgDataType type = cluster.getTypeFactory().createTypeWithNullability( cluster.getTypeFactory().createPolyType( PolyType.BOOLEAN ), true );
        if ( bsonValue.isArray() ) {
            List<RexNode> arr = convertArray( parentKey, bsonValue.asArray(), true, rowType, "" );
            RexNode id = getIdentifier( parentKey, rowType );

            List<RexNode> operands = new ArrayList<>();
            for ( RexNode rexNode : arr ) {
                operands.add( new RexCall( type, OperatorRegistry.get( QueryLanguage.from( MONGO ), OperatorName.MQL_EQUALS ), Arrays.asList( id, rexNode ) ) );
            }

            return getFixedCall( operands, OperatorRegistry.get( OperatorName.AND ), PolyType.BOOLEAN );
        } else {
            throw new GenericRuntimeException( "After $all there needs to follow a array" );
        }
    }


    /**
     * Converts an $type field, this only the field if it matches the provided type
     * <pre>
     *     { field: { $type: <BSON type> } }
     * </pre>
     * or
     * <pre>
     *     { field: { $type: [ <BSON type1> , <BSON type2>, ... ] } }
     * </pre>
     *
     * @param value the information of the $type in BSON format
     * @param parentKey the name of the parent key
     * @param rowType the row information of the filtered node
     * @return a node with the applied filter
     */
    private RexNode convertType( BsonValue value, String parentKey, AlgDataType rowType ) {
        String errorMsg = "$type needs either a array of type names or numbers or a single number";
        RexCall types;
        if ( value.isArray() ) {
            List<Integer> numbers = new ArrayList<>();
            for ( BsonValue bsonValue : value.asArray() ) {
                if ( bsonValue.isString() || bsonValue.isNumber() ) {
                    numbers.add( bsonValue.isNumber() ? bsonValue.asNumber().intValue() : DocumentUtil.getTypeNumber( bsonValue.asString().getValue() ) );
                } else {
                    throw new GenericRuntimeException( errorMsg );
                }
            }
            types = getIntArray( DocumentUtil.removePlaceholderTypes( numbers ) );
        } else if ( value.isNumber() || value.isString() ) {
            int typeNumber = value.isNumber() ? value.asNumber().intValue() : DocumentUtil.getTypeNumber( value.asString().getValue() );
            types = getIntArray( DocumentUtil.removePlaceholderTypes( Collections.singletonList( typeNumber ) ) );
        } else {
            throw new GenericRuntimeException( errorMsg );
        }
        return new RexCall(
                cluster.getTypeFactory().createPolyType( PolyType.BOOLEAN ),
                OperatorRegistry.get( QueryLanguage.from( MONGO ), OperatorName.MQL_TYPE_MATCH ),
                Arrays.asList( getIdentifier( parentKey, rowType ), types ) );

    }


    /**
     * Inverts the provided node by wrapping it in a NOT
     *
     * @param node the node, which is wrapped
     * @return the wrapped node
     */
    private RexNode negate( RexNode node ) {
        return new RexCall( cluster.getTypeFactory().createPolyType( PolyType.BOOLEAN ), OperatorRegistry.get( OperatorName.NOT ), Collections.singletonList( node ) );
    }


    /**
     * Specifies where an underlying field is located in a document.
     * <pre>
     *  {
     *      key1: val1,
     *      key2: {
     *          key22: "test"
     *      }
     *  }
     * </pre>
     * on retrieval of key22:
     * <pre>
     *     DOC_QUERY_VALUE(["key2", "key22"])
     * </pre>
     *
     * @param index positions of the column in which the parent document is stored
     * @param key the key in the form "key2.key22"
     * @return the node, which defines the retrieval of the underlying key
     */
    public RexNode translateDocValue( @Nullable Integer index, String key ) {
        //RexCall filter;
        List<String> names = Arrays.asList( key.split( "\\." ) );

        return this.subElement.orElseGet( () -> new RexNameRef( names, index, DocumentType.ofDoc() ) );
    }


    private RexCall getIntArray( List<Integer> elements ) {
        List<RexNode> rexNodes = new ArrayList<>();
        for ( Integer name : elements ) {
            rexNodes.add( convertLiteral( new BsonInt32( name ) ) );
        }

        AlgDataType type = cluster.getTypeFactory().createArrayType(
                cluster.getTypeFactory().createPolyType( PolyType.INTEGER ),
                rexNodes.size() );
        return DocumentUtil.getArray( rexNodes, type );
    }


    private RexCall getStringArray( List<String> elements ) {
        List<RexNode> rexNodes = new ArrayList<>();
        int maxSize = 0;
        AlgDataType type = cluster.getTypeFactory().createPolyType( PolyType.CHAR, 200 );
        for ( String name : elements ) {
            rexNodes.add( new RexLiteral( PolyString.of( name ), type, PolyType.CHAR ) );
            maxSize = Math.max( name.length(), maxSize );
        }

        AlgDataType arrayType = cluster.getTypeFactory().createArrayType(
                cluster.getTypeFactory().createPolyType( PolyType.CHAR, maxSize ),
                rexNodes.size() );
        return DocumentUtil.getArray( rexNodes, arrayType );
    }


    private RexNode convertLiteral( BsonValue bsonValue ) {
        Pair<PolyValue, PolyType> valuePair = RexLiteral.convertType( getPolyValue( bsonValue ), new DocumentType() );
        return new RexLiteral( valuePair.left, new DocumentType(), valuePair.right );

    }


    /**
     * Converts an $in or $nin operation, by transforming it to an OR with all values
     *
     * @param bsonValue the value, which specifies the $in operation
     * @param op if the operation is an $in or $nin operation
     * @param key the field, which values have to be present in
     * @param rowType the row information of the collection/table
     * @return the transformed $in operation
     */
    private RexNode convertIn( BsonValue bsonValue, Operator op, String key, AlgDataType rowType ) {
        AlgDataType type = cluster.getTypeFactory().createTypeWithNullability( cluster.getTypeFactory().createPolyType( PolyType.BOOLEAN ), true );

        List<RexNode> operands = new ArrayList<>();
        boolean isIn = op.getOperatorName() == OperatorName.IN;
        op = isIn ? OperatorRegistry.get( OperatorName.OR ) : OperatorRegistry.get( OperatorName.AND );
        RexNode id = getIdentifier( key, rowType );

        for ( BsonValue literal : bsonValue.asArray() ) {
            if ( literal.isDocument() ) {
                throw new GenericRuntimeException( "Non-literal in $in clauses are not supported" );
            }
            if ( literal.isRegularExpression() ) {
                RexNode filter = getRegex( literal.asRegularExpression().getPattern(), literal.asRegularExpression().getOptions(), key, rowType );
                if ( !isIn ) {
                    filter = negate( filter );
                }
                operands.add( filter );
            } else {
                operands.add( new RexCall( type, isIn ? OperatorRegistry.get( QueryLanguage.from( MONGO ), OperatorName.MQL_EQUALS ) : OperatorRegistry.get( OperatorName.NOT_EQUALS ), Arrays.asList( id, convertLiteral( literal ) ) ) );
            }
        }

        return getFixedCall( operands, op, PolyType.BOOLEAN );
    }


    /**
     * Starts translation of an aggregation stage <code>$project</code> or a projection in a <code>db.collection.find({},{projection})</code>
     *
     * @param projectionValue initial unparsed BSON, which defines the projection
     * @param node the node to which the projection is applied
     * @param rowType the rowType, which is used at the moment
     * @param isAddFields if the projection is a <code>$addFields</code>, which is basically only inclusive projections
     * @param isUnset if the projection is a <code>$unset</code>, which is exclusive projection
     * @return the projected node
     */
    private AlgNode combineProjection( BsonValue projectionValue, AlgNode node, AlgDataType rowType, boolean isAddFields, boolean isUnset ) {
        Map<String, RexNode> includes = new HashMap<>();
        List<String> excludes = new ArrayList<>();

        BsonDocument projection;
        if ( projectionValue.isDocument() ) {
            projection = projectionValue.asDocument();
            translateProjection( rowType, isAddFields, isUnset, includes, excludes, projection );
        } else if ( projectionValue.isArray() || projectionValue.isString() ) {
            List<BsonValue> array;
            if ( projectionValue.isArray() ) {
                array = projectionValue.asArray();
            } else if ( projectionValue.isString() ) {
                array = new BsonArray( Collections.singletonList( projectionValue.asString() ) );
            } else {
                throw new GenericRuntimeException( "$unset or $addFields needs a string or an array of strings" );
            }

            for ( BsonValue value : array ) {
                if ( isUnset ) {
                    excludes.add( value.asString().getValue() );
                } else {
                    includes.put( value.asString().getValue(), getIdentifier( value.asString().getValue(), rowType ) );
                }
            }


        } else {
            throw new GenericRuntimeException( "The provided projection was not translatable" );
        }

        if ( !includes.isEmpty() && !excludes.isEmpty() ) {
            throw new GenericRuntimeException( "Include projection and exclude projections are not possible at the same time." );
        }

        if ( !excludes.isEmpty() ) {
            return LogicalDocumentProject.create( node, new HashMap<>(), excludes );

        } else if ( isAddFields ) {
            List<String> names = new ArrayList<>();
            names.add( null ); // we want it at the root

            // we have to implement the added fields into the _data field
            // as this is later used to retrieve them when projecting

            //int dataIndex = rowType.getFieldNames().indexOf( "d" );

            for ( Entry<String, RexNode> entry : includes.entrySet() ) {
                List<RexNode> values = new ArrayList<>();

                // we attach the new values to the input bson
                values.add( new RexCall(
                        any,
                        OperatorRegistry.get( QueryLanguage.from( MONGO ), OperatorName.MQL_ADD_FIELDS ),
                        Arrays.asList(
                                RexIndexRef.of( 0, rowType ),
                                cluster.getRexBuilder().makeArray( cluster.getTypeFactory().createArrayType( cluster.getTypeFactory().createPolyType( PolyType.CHAR, 255 ), -1 ),
                                        PolyList.copyOf( Arrays.stream( entry.getKey().split( "\\." ) ).map( PolyString::of ).collect( Collectors.toList() ) ) ),
                                entry.getValue() ) ) );

                node = LogicalDocumentProject.create( node, values, names );
            }

            return node;
        } else if ( !includes.isEmpty() ) {

            if ( !includes.containsKey( DocumentType.DOCUMENT_ID ) ) {
                includes.put( DocumentType.DOCUMENT_ID, getIdentifier( DocumentType.DOCUMENT_ID, rowType ) );
            }

            return LogicalDocumentProject.create( node, new ArrayList<>( includes.values() ), new ArrayList<>( includes.keySet() ) );
        }
        return node;
    }


    /**
     * Finalizes a projection translations
     *
     * @param rowType the rowType, which is used at the moment
     * @param isAddFields if the projection is a <code>$addFields</code>, which is basically only inclusive projections
     * @param isUnset if the projection is a <code>$unset</code>, which is exclusive projection
     * @param includes which fields are includes, as in <code>"field":1</code>
     * @param excludes which fields are includes, as in <code>"field":0</code>
     * @param projection document, which defines the structure of the projection
     */
    private void translateProjection( AlgDataType rowType, boolean isAddFields, boolean isUnset, Map<String, RexNode> includes, List<String> excludes, BsonDocument projection ) {
        for ( Entry<String, BsonValue> entry : projection.entrySet() ) {
            BsonValue value = entry.getValue();
            if ( value.isNumber() && !isAddFields ) {
                // we have a simple projection; [name]: 1 (include) or [name]:0 (exclude)
                if ( value.asNumber().intValue() == 1 ) {
                    includes.put( entry.getKey(), getIdentifier( entry.getKey(), rowType ) );
                } else if ( value.asNumber().intValue() == 0 ) {
                    excludes.add( entry.getKey() );
                }

            } else if ( value.isString() && value.asString().getValue().startsWith( "$" ) ) {
                // we have a renaming; [new name]: $[old name] ( this counts as a inclusion projection
                String oldName = value.asString().getValue().substring( 1 );

                includes.put( entry.getKey(), getIdentifier( oldName, rowType ) );
            } else if ( value.isDocument() && value.asDocument().size() == 1 && value.asDocument().getFirstKey().startsWith( "$" ) ) {
                String func = value.asDocument().getFirstKey();
                if ( mathOperators.containsKey( func ) ) {
                    includes.put( entry.getKey(), convertMath( func, entry.getKey(), value.asDocument().get( func ), rowType, false ) );
                } else if ( func.equals( "$arrayElemAt" ) ) {
                    includes.put( entry.getKey(), convertArrayAt( entry.getKey(), value.asDocument().get( func ), rowType ) );
                } else if ( func.equals( "$slice" ) ) {
                    includes.put( entry.getKey(), convertSlice( entry.getKey(), value.asDocument().get( func ), rowType ) );
                } else if ( func.equals( "$literal" ) ) {
                    if ( value.asDocument().get( func ).isInt32() && value.asDocument().get( func ).asInt32().getValue() == 0 ) {
                        excludes.add( entry.getKey() );
                    } else {
                        includes.put( entry.getKey(), getIdentifier( entry.getKey(), rowType ) );
                    }
                }
            } else if ( isAddFields && !value.isDocument() ) {
                if ( value.isArray() ) {
                    List<RexNode> nodes = new ArrayList<>();
                    for ( BsonValue bsonValue : value.asArray() ) {
                        nodes.add( convertLiteral( bsonValue ) );
                    }
                    includes.put( entry.getKey(), DocumentUtil.getArray( nodes, any ) );
                } else {
                    includes.put( entry.getKey(), convertLiteral( value ) );
                }
            } else if ( isUnset && value.isArray() ) {
                for ( BsonValue bsonValue : value.asArray() ) {
                    if ( bsonValue.isString() ) {
                        excludes.add( bsonValue.asString().getValue() );
                    } else {
                        throw new GenericRuntimeException( "When using $unset with an array, it can only contain strings" );
                    }
                }
            } else {
                String msg;
                if ( !isAddFields ) {
                    msg = "After a projection there needs to be either a number, a renaming, a literal or a function.";
                } else {
                    msg = "After a projection there needs to be either a renaming, a literal or a function.";
                }
                throw new GenericRuntimeException( msg );
            }
        }
    }


}
