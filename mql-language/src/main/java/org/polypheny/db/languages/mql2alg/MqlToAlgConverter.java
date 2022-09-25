/*
 * Copyright 2019-2022 The Polypheny Project
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

import com.google.common.collect.ImmutableList;
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
import java.util.function.BiFunction;
import java.util.stream.Collectors;
import lombok.Getter;
import org.bson.BsonArray;
import org.bson.BsonBoolean;
import org.bson.BsonDocument;
import org.bson.BsonInt32;
import org.bson.BsonNumber;
import org.bson.BsonRegularExpression;
import org.bson.BsonString;
import org.bson.BsonValue;
import org.polypheny.db.algebra.AlgCollation;
import org.polypheny.db.algebra.AlgCollations;
import org.polypheny.db.algebra.AlgFieldCollation;
import org.polypheny.db.algebra.AlgFieldCollation.Direction;
import org.polypheny.db.algebra.AlgNode;
import org.polypheny.db.algebra.AlgRoot;
import org.polypheny.db.algebra.constant.Kind;
import org.polypheny.db.algebra.core.AggregateCall;
import org.polypheny.db.algebra.core.CorrelationId;
import org.polypheny.db.algebra.core.Modify.Operation;
import org.polypheny.db.algebra.core.Scan;
import org.polypheny.db.algebra.core.Values;
import org.polypheny.db.algebra.core.document.DocumentProject;
import org.polypheny.db.algebra.fun.AggFunction;
import org.polypheny.db.algebra.logical.document.LogicalDocumentAggregate;
import org.polypheny.db.algebra.logical.document.LogicalDocumentFilter;
import org.polypheny.db.algebra.logical.document.LogicalDocumentModify;
import org.polypheny.db.algebra.logical.document.LogicalDocumentProject;
import org.polypheny.db.algebra.logical.document.LogicalDocumentScan;
import org.polypheny.db.algebra.logical.document.LogicalDocumentSort;
import org.polypheny.db.algebra.logical.document.LogicalDocumentValues;
import org.polypheny.db.algebra.operators.OperatorName;
import org.polypheny.db.algebra.type.AlgDataType;
import org.polypheny.db.algebra.type.AlgDataTypeFactory;
import org.polypheny.db.algebra.type.AlgDataTypeFactory.Builder;
import org.polypheny.db.algebra.type.AlgDataTypeField;
import org.polypheny.db.algebra.type.AlgDataTypeFieldImpl;
import org.polypheny.db.algebra.type.AlgDataTypeImpl;
import org.polypheny.db.algebra.type.AlgDataTypeSystem;
import org.polypheny.db.catalog.Catalog;
import org.polypheny.db.catalog.Catalog.NamespaceType;
import org.polypheny.db.catalog.Catalog.Pattern;
import org.polypheny.db.catalog.Catalog.QueryLanguage;
import org.polypheny.db.languages.OperatorRegistry;
import org.polypheny.db.languages.QueryParameters;
import org.polypheny.db.languages.mql.Mql.Type;
import org.polypheny.db.languages.mql.MqlAggregate;
import org.polypheny.db.languages.mql.MqlCollectionStatement;
import org.polypheny.db.languages.mql.MqlCount;
import org.polypheny.db.languages.mql.MqlDelete;
import org.polypheny.db.languages.mql.MqlFind;
import org.polypheny.db.languages.mql.MqlInsert;
import org.polypheny.db.languages.mql.MqlQueryParameters;
import org.polypheny.db.languages.mql.MqlQueryStatement;
import org.polypheny.db.languages.mql.MqlUpdate;
import org.polypheny.db.nodes.Node;
import org.polypheny.db.nodes.Operator;
import org.polypheny.db.plan.AlgOptCluster;
import org.polypheny.db.plan.AlgOptTable;
import org.polypheny.db.prepare.AlgOptTableImpl;
import org.polypheny.db.prepare.PolyphenyDbCatalogReader;
import org.polypheny.db.prepare.Prepare.PreparingTable;
import org.polypheny.db.processing.Processor;
import org.polypheny.db.rex.RexBuilder;
import org.polypheny.db.rex.RexCall;
import org.polypheny.db.rex.RexInputRef;
import org.polypheny.db.rex.RexLiteral;
import org.polypheny.db.rex.RexNode;
import org.polypheny.db.schema.LogicalTable;
import org.polypheny.db.schema.PolyphenyDbSchema.TableEntryImpl;
import org.polypheny.db.schema.document.DocumentUtil;
import org.polypheny.db.type.PolyType;
import org.polypheny.db.type.PolyTypeFactoryImpl;
import org.polypheny.db.util.DateString;
import org.polypheny.db.util.ImmutableBitSet;
import org.polypheny.db.util.Pair;
import org.polypheny.db.util.TimestampString;


/**
 * Converter class, which transforms a MongoQL command in its MqlNode form to an equal AlgNode
 */
public class MqlToAlgConverter {

    private final PolyphenyDbCatalogReader catalogReader;
    private final AlgOptCluster cluster;
    private RexBuilder builder;
    private final static Map<String, Operator> mappings;
    private final static List<String> operators;
    private final static Map<String, List<Operator>> gates;
    private final static Map<String, Operator> mathOperators;
    private static final Map<String, AggFunction> accumulators;
    private final AlgDataType any;
    private final AlgDataType nullableAny;

    private final AlgDataType jsonType;
    private final AlgDataType nullableJsonType;


    private static final HashMap<String, Operator> singleMathOperators;


    private static final HashMap<String, Operator> mathComparators;


    static {
        gates = new HashMap<>();
        gates.put( "$and", Collections.singletonList( OperatorRegistry.get( OperatorName.AND ) ) );
        gates.put( "$or", Collections.singletonList( OperatorRegistry.get( OperatorName.OR ) ) );
        gates.put( "$nor", Arrays.asList( OperatorRegistry.get( OperatorName.AND ), OperatorRegistry.get( OperatorName.NOT ) ) );
        gates.put( "$not", Collections.singletonList( OperatorRegistry.get( OperatorName.NOT ) ) );

        mathComparators = new HashMap<>();

        mappings = new HashMap<>();

        mappings.put( "$lt", OperatorRegistry.get( QueryLanguage.MONGO_QL, OperatorName.MQL_LT ) );
        mappings.put( "$gt", OperatorRegistry.get( QueryLanguage.MONGO_QL, OperatorName.MQL_GT ) );
        mappings.put( "$lte", OperatorRegistry.get( QueryLanguage.MONGO_QL, OperatorName.MQL_LTE ) );
        mappings.put( "$gte", OperatorRegistry.get( QueryLanguage.MONGO_QL, OperatorName.MQL_GTE ) );

        mathComparators.putAll( mappings );

        mappings.put( "$eq", OperatorRegistry.get( QueryLanguage.MONGO_QL, OperatorName.MQL_EQUALS ) );
        mappings.put( "$ne", OperatorRegistry.get( OperatorName.NOT_EQUALS ) );
        mappings.put( "$in", OperatorRegistry.get( OperatorName.IN ) );
        mappings.put( "$nin", OperatorRegistry.get( OperatorName.NOT_IN ) );

        mappings.put( "$exists", OperatorRegistry.get( OperatorName.EXISTS ) );

        mathOperators = new HashMap<>();
        mathOperators.put( "$subtract", OperatorRegistry.get( OperatorName.MINUS ) );
        mathOperators.put( "$add", OperatorRegistry.get( OperatorName.PLUS ) );
        mathOperators.put( "$multiply", OperatorRegistry.get( OperatorName.MULTIPLY ) );
        mathOperators.put( "$divide", OperatorRegistry.get( OperatorName.DIVIDE ) );
        mathOperators.put( "$mod", OperatorRegistry.get( OperatorName.MOD ) );
        mathOperators.put( "$pow", OperatorRegistry.get( OperatorName.POWER ) );
        mathOperators.put( "$sum", OperatorRegistry.get( OperatorName.SUM ) );
        mathOperators.put( "$literal", null );

        singleMathOperators = new HashMap<>();
        singleMathOperators.put( "$abs", OperatorRegistry.get( OperatorName.ABS ) );
        singleMathOperators.put( "$acos", OperatorRegistry.get( OperatorName.ACOS ) );
        //singleMathOperators.put( "$acosh", StdOperatorRegistry.get( OperatorName.ACOSH ) );
        singleMathOperators.put( "$asin", OperatorRegistry.get( OperatorName.ASIN ) );
        singleMathOperators.put( "$atan", OperatorRegistry.get( OperatorName.ATAN ) );
        singleMathOperators.put( "$atan2", OperatorRegistry.get( OperatorName.ATAN2 ) );
        //singleMathOperators.put( "$atanh", StdOperatorRegistry.get( OperatorName.ATANH ) );
        singleMathOperators.put( "$ceil", OperatorRegistry.get( OperatorName.CEIL ) );
        singleMathOperators.put( "$cos", OperatorRegistry.get( OperatorName.COS ) );
        //singleMathOperators.put( "$cosh", StdOperatorRegistry.get( OperatorName.COSH ) );
        singleMathOperators.put( "$degreesToRadians", OperatorRegistry.get( OperatorName.DEGREES ) );
        singleMathOperators.put( "$floor", OperatorRegistry.get( OperatorName.FLOOR ) );
        singleMathOperators.put( "$ln", OperatorRegistry.get( OperatorName.LN ) );
        singleMathOperators.put( "$log", OperatorRegistry.get( OperatorName.LN ) );
        singleMathOperators.put( "$log10", OperatorRegistry.get( OperatorName.LOG10 ) );
        singleMathOperators.put( "$sin", OperatorRegistry.get( OperatorName.SIN ) );
        //singleMathOperators.put( "$sinh", StdOperatorRegistry.get( OperatorName.SINH ) );
        singleMathOperators.put( "$sqrt", OperatorRegistry.get( OperatorName.SQRT ) );
        singleMathOperators.put( "$tan", OperatorRegistry.get( OperatorName.TAN ) );
        //singleMathOperators.put( "$tanh", StdOperatorRegistry.get( OperatorName.TANH ) );

        operators = new ArrayList<>();
        operators.addAll( mappings.keySet() );
        operators.addAll( gates.keySet() );
        operators.addAll( mathOperators.keySet() );
        operators.addAll( singleMathOperators.keySet() );

        accumulators = new HashMap<>();
        //$addToSet
        accumulators.put( "$avg", OperatorRegistry.getAgg( OperatorName.AVG ) );
        accumulators.put( "$count", OperatorRegistry.getAgg( OperatorName.COUNT ) );
        accumulators.put( "$first", OperatorRegistry.getAgg( OperatorName.FIRST_VALUE ) );
        accumulators.put( "$last", OperatorRegistry.getAgg( OperatorName.LAST_VALUE ) );
        accumulators.put( "$max", OperatorRegistry.getAgg( OperatorName.MAX ) );
        //$mergeObjects
        accumulators.put( "$min", OperatorRegistry.getAgg( OperatorName.MIN ) );
        //$push
        accumulators.put( "$stdDevPop", OperatorRegistry.getAgg( OperatorName.STDDEV_POP ) );
        accumulators.put( "$stdDevSamp", OperatorRegistry.getAgg( OperatorName.STDDEV_SAMP ) );
        accumulators.put( "$sum", OperatorRegistry.getAgg( OperatorName.SUM ) );

        // special cases
        operators.add( "$type" );
        operators.add( "$expr" );
        operators.add( "$jsonSchema" );
        operators.add( "$all" );
        operators.add( "$elemMatch" );
        operators.add( "$size" );
    }


    private boolean excludedId = false;
    private boolean _dataExists = true;
    private boolean elemMatchActive = false;
    private String defaultDatabase;
    private boolean notActive = false;
    private boolean usesDocumentModel;
    private AlgOptTable entity;
    private MqlQueryParameters parameters;


    public MqlToAlgConverter( Processor mqlProcessor, PolyphenyDbCatalogReader catalogReader, AlgOptCluster cluster ) {
        this.catalogReader = catalogReader;
        this.cluster = Objects.requireNonNull( cluster );
        this.any = this.cluster.getTypeFactory().createPolyType( PolyType.ANY );
        this.nullableAny = this.cluster.getTypeFactory().createTypeWithNullability( any, true );

        this.jsonType = this.cluster.getTypeFactory().createPolyType( PolyType.JSON );
        this.nullableJsonType = this.cluster.getTypeFactory().createTypeWithNullability( jsonType, true );

        resetDefaults();
    }


    /**
     * This class is reused and has to reset these values each time this happens
     */
    private void resetDefaults() {
        excludedId = false;
        _dataExists = true;
        notActive = false;
        elemMatchActive = false;
        entity = null;
    }


    public AlgRoot convert( Node query, QueryParameters parameters ) {
        resetDefaults();
        this.parameters = (MqlQueryParameters) parameters;
        this.defaultDatabase = ((MqlQueryParameters) parameters).getDatabase();
        if ( query instanceof MqlCollectionStatement ) {
            return convert( (MqlCollectionStatement) query );
        }
        throw new RuntimeException( "DML or DQL need a collection" );
    }


    /**
     * Converts the initial MongoQl by stepping through it iteratively
     *
     * @param query the query in MqlNode format
     * @return the {@link AlgNode} format of the initial query
     */
    public AlgRoot convert( MqlCollectionStatement query ) {
        Type kind = query.getMqlKind();
        this.entity = getEntity( query, defaultDatabase );
        if ( entity == null ) {
            throw new RuntimeException( "The used collection does not exist." );
        }

        AlgNode node;

        if ( entity.getTable().getSchemaType() == NamespaceType.RELATIONAL ) {
            _dataExists = false;
        }

        node = LogicalDocumentScan.create( cluster, entity );
        this.usesDocumentModel = true;

        AlgDataType rowType = entity.getRowType();

        this.builder = new RexBuilder( cluster.getTypeFactory() );

        AlgRoot root;

        switch ( kind ) {
            case FIND:
                AlgNode find = convertFind( (MqlFind) query, rowType, node );
                root = AlgRoot.of( find, find.getRowType(), Kind.SELECT );
                break;
            case COUNT:
                AlgNode count = convertCount( (MqlCount) query, rowType, node );
                root = AlgRoot.of( count, count.getRowType(), Kind.SELECT );
                break;
            case AGGREGATE:
                AlgNode aggregate = convertAggregate( (MqlAggregate) query, rowType, node );
                root = AlgRoot.of( aggregate, Kind.SELECT );
                break;
            /// dmls
            case INSERT:
                root = AlgRoot.of( convertInsert( (MqlInsert) query, entity ), Kind.INSERT );
                break;
            case DELETE:
            case FIND_DELETE:
                root = AlgRoot.of( convertDelete( (MqlDelete) query, entity, node ), Kind.DELETE );
                break;
            case UPDATE:
                root = AlgRoot.of( convertUpdate( (MqlUpdate) query, entity, node ), Kind.UPDATE );
                break;
            default:
                throw new IllegalStateException( "Unexpected value: " + kind );
        }
        /*if ( usesDocumentModel ) {
            root.usesDocumentModel = true;
        }*/
        return root;
    }


    private AlgOptTable getEntity( MqlCollectionStatement query, String dbSchemaName ) {
        List<String> names = ImmutableList.of( dbSchemaName, query.getCollection() );

        PreparingTable table = catalogReader.getTable( names );

        if ( table == null ) {
            return catalogReader.getCollection( names );
        } else if ( table.getTable().getSchemaType() == NamespaceType.GRAPH ) {

            final AlgDataTypeFactory typeFactory = new PolyTypeFactoryImpl( AlgDataTypeSystem.DEFAULT );

            final Builder fieldInfo = typeFactory.builder();
            //fieldInfo.add( new AlgDataTypeFieldImpl( "_id", 0, typeFactory.createPolyType( PolyType.VARCHAR, 24 ) ) );
            fieldInfo.add( new AlgDataTypeFieldImpl( "d", 0, typeFactory.createPolyType( PolyType.DOCUMENT ) ) );
            AlgDataType rowType = fieldInfo.build();

            return AlgOptTableImpl.create(
                    table.getRelOptSchema(),
                    rowType,
                    new TableEntryImpl(
                            catalogReader.getRootSchema(), names.get( names.size() - 1 ),
                            new LogicalTable( Catalog.getInstance().getSchemas( Catalog.defaultDatabaseId, new Pattern( dbSchemaName ) ).get( 0 ).id, names.get( 0 ), names.get( names.size() - 1 ), List.of(), List.of(), AlgDataTypeImpl.proto( rowType ), NamespaceType.GRAPH ) ),
                    1.0 );
        }

        return table;
    }


    /**
     * Starts converting a db.collection.update();
     */
    private AlgNode convertUpdate( MqlUpdate query, AlgOptTable table, AlgNode node ) {
        if ( !query.getQuery().isEmpty() ) {
            node = convertQuery( query, table.getRowType(), node );
            if ( query.isOnlyOne() ) {
                node = wrapLimit( node, 1 );
            }
        }
        if ( query.isUsesPipeline() ) {
            node = convertReducedPipeline( query, table.getRowType(), node, table );
        } else {
            node = translateUpdate( query, table.getRowType(), node, table );
        }

        return node;
    }


    /**
     * Translates an update document
     *
     * this method is implemented like the reduced update pipeline,
     * but in fact could be combined and therefore optimized a lot more
     */
    private AlgNode translateUpdate( MqlUpdate query, AlgDataType rowType, AlgNode node, AlgOptTable table ) {
        Map<String, RexNode> updates = new HashMap<>();
        Map<UpdateOperation, List<Pair<String, RexNode>>> mergedUpdates = new HashMap<>();
        mergedUpdates.put( UpdateOperation.REMOVE, new ArrayList<>() );
        mergedUpdates.put( UpdateOperation.RENAME, new ArrayList<>() );
        mergedUpdates.put( UpdateOperation.REPLACE, new ArrayList<>() );

        UpdateOperation updateOp;
        for ( Entry<String, BsonValue> entry : query.getUpdate().asDocument().entrySet() ) {
            String op = entry.getKey();
            if ( !entry.getValue().isDocument() ) {
                throw new RuntimeException( "After a update statement a document is needed" );
            }

            switch ( op ) {
                case ("$currentDate"):
                    updates.putAll( translateCurrentDate( entry.getValue().asDocument(), rowType ) );
                    updateOp = UpdateOperation.REPLACE;
                    break;
                case "$inc":
                    updates.putAll( translateInc( entry.getValue().asDocument(), rowType ) );
                    updateOp = UpdateOperation.REPLACE;
                    break;
                case "$min":
                    updates.putAll( translateMinMaxMul( entry.getValue().asDocument(), rowType, OperatorRegistry.get( QueryLanguage.MONGO_QL, OperatorName.MQL_UPDATE_MIN ) ) );
                    updateOp = UpdateOperation.REPLACE;
                    break;
                case "$max":
                    updates.putAll( translateMinMaxMul( entry.getValue().asDocument(), rowType, OperatorRegistry.get( QueryLanguage.MONGO_QL, OperatorName.MQL_UPDATE_MAX ) ) );
                    updateOp = UpdateOperation.REPLACE;
                    break;
                case "$mul":
                    updates.putAll( translateMinMaxMul( entry.getValue().asDocument(), rowType, OperatorRegistry.get( OperatorName.MULTIPLY ) ) );
                    updateOp = UpdateOperation.REPLACE;
                    break;
                case "$rename":
                    updates.putAll( translateRename( entry.getValue().asDocument(), rowType ) );
                    updateOp = UpdateOperation.RENAME;
                    break;
                case "$set":
                    updates.putAll( translateSet( entry.getValue().asDocument(), rowType ) );
                    updateOp = UpdateOperation.REPLACE;
                    break;
                /*case ("$setOnInsert"):
                    updates.putAll( translateSet(  ) );*/
                case "$unset":
                    updates.putAll( translateUnset( entry.getValue().asDocument(), rowType ) );
                    updateOp = UpdateOperation.REMOVE;
                    break;
                case "$addToSet":
                    updates.putAll( translateAddToSet( entry.getValue().asDocument(), rowType ) );
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
                    throw new RuntimeException( "The update operation is not supported." );
            }
            if ( query.isOnlyOne() ) {
                node = wrapLimit( node, 1 );
            }

            mergeUpdates( mergedUpdates, rowType, updates, updateOp );
            updates.clear();
        }

        return finalizeUpdates( "d", mergedUpdates, rowType, node, table );


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
                    throw new RuntimeException( "You cannot rename a fixed field in an update, as this is a ddl" );
                    // TODO DL maybe find way to trigger ddl later
                }
            } else {
                String childName = null;
                if ( names.contains( parent ) ) {
                    List<String> childNames = Arrays.asList( splits );
                    childNames.remove( 0 );
                    childName = String.join( ".", childNames );
                } else if ( _dataExists ) {
                    parent = "_data";
                    childName = nodeEntry.getKey();
                }

                if ( childName == null ) {
                    throw new RuntimeException( "the specified field in the update was not found" );
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

        if ( !Collections.disjoint( directUpdates.entrySet(), childUpdates.keySet() ) && directUpdates.size() == 0 ) {
            throw new RuntimeException( "DML of a field and its subfields at the same time is not possible" );
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
                                    .collect( Collectors.toList() ) );
        }

    }


    /**
     * Updates contain RENAME, REMOVE, REPLACE parts and are merged into a single DOC_UPDATE in this method
     *
     * @param key the left associated parent key
     * @param mergedUpdates collection, which combines all performed update steps according to the operation
     * @param rowType the default rowtype at this point
     * @param node the transformed operation up to this step e.g. {@link Scan} or {@link LogicalDocumentAggregate}
     * @param table the active table
     * @return the unified UPDATE AlgNode
     */
    private AlgNode finalizeUpdates( String key, Map<UpdateOperation, List<Pair<String, RexNode>>> mergedUpdates, AlgDataType rowType, AlgNode node, AlgOptTable table ) {
        RexNode updateChain = getIdentifier( key, rowType );
        // replace
        List<Pair<String, RexNode>> replaceNodes = mergedUpdates.get( UpdateOperation.REPLACE );

        if ( replaceNodes.size() > 0 ) {
            updateChain = new RexCall(
                    jsonType,
                    OperatorRegistry.get( QueryLanguage.MONGO_QL, OperatorName.MQL_UPDATE_REPLACE ),
                    Arrays.asList(
                            updateChain,
                            getStringArray( Pair.left( replaceNodes ) ),
                            getArray( Pair.right( replaceNodes ), jsonType ) ) );
        }

        // rename
        List<Pair<String, RexNode>> renameNodes = mergedUpdates.get( UpdateOperation.RENAME );

        if ( renameNodes.size() > 0 ) {
            updateChain = new RexCall(
                    jsonType,
                    OperatorRegistry.get( QueryLanguage.MONGO_QL, OperatorName.MQL_UPDATE_RENAME ),
                    Arrays.asList(
                            updateChain,
                            getStringArray( Pair.left( renameNodes ) ),
                            getArray( Pair.right( renameNodes ), jsonType ) ) );
        }

        // remove
        List<String> removeNodes = Pair.left( mergedUpdates.get( UpdateOperation.REMOVE ) );

        if ( removeNodes.size() > 0 ) {
            updateChain = new RexCall(
                    jsonType,
                    OperatorRegistry.get( QueryLanguage.MONGO_QL, OperatorName.MQL_UPDATE_REMOVE ),
                    Arrays.asList(
                            updateChain,
                            getStringArray( removeNodes ) ) );
        }

        if ( removeNodes.size() > 0 ) {
            updateChain = new RexCall(
                    jsonType,
                    OperatorRegistry.get(
                            QueryLanguage.MONGO_QL,
                            OperatorName.MQL_UPDATE ),
                    Arrays.asList(
                            updateChain,
                            getStringArray( removeNodes ) ) );
        }

        return LogicalDocumentModify.create(
                table,
                node,
                catalogReader,
                Operation.UPDATE,
                Collections.singletonList( key ),
                Collections.singletonList( createJsonify( updateChain ) ) );
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
            RexCall addToSet = new RexCall( jsonType, OperatorRegistry.get( QueryLanguage.MONGO_QL, OperatorName.MQL_UPDATE_ADD_TO_SET ), Arrays.asList( id, value ) );

            updates.put( entry.getKey(), addToSet );
        }
        return updates;
    }


    /**
     * Start translation of db.collection.update({$unset: "key"})
     * this excludes the defined key from the document
     */
    private Map<String, RexNode> translateUnset( BsonDocument doc, AlgDataType rowType ) {
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
    private Map<String, RexNode> translateRename( BsonDocument doc, AlgDataType rowType ) {
        Map<String, RexNode> updates = new HashMap<>();
        for ( Entry<String, BsonValue> entry : doc.entrySet() ) {
            RexLiteral literal = builder.makeLiteral( entry.getValue().asString().getValue() );
            updates.put( entry.getKey(), literal );
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
            updates.put( entry.getKey(), builder.makeCall( OperatorRegistry.get( OperatorName.PLUS ), id, literal ) );
        }
        return updates;
    }


    /**
     * Start translation of db.collection.update({$currentDate: {"key": {"$type": "timestamp"}}})
     * this replaces the value of the given key, with the current date in timestamp or date format
     */
    private Map<String, RexNode> translateCurrentDate( BsonDocument value, AlgDataType rowType ) {
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
    private AlgNode convertReducedPipeline( MqlUpdate query, AlgDataType rowType, AlgNode node, AlgOptTable table ) {
        Map<String, RexNode> updates = new HashMap<>();
        Map<UpdateOperation, List<Pair<String, RexNode>>> mergedUpdates = new HashMap<>();
        mergedUpdates.put( UpdateOperation.REMOVE, new ArrayList<>() );
        mergedUpdates.put( UpdateOperation.RENAME, new ArrayList<>() );
        mergedUpdates.put( UpdateOperation.REPLACE, new ArrayList<>() );

        UpdateOperation updateOp;
        for ( BsonValue value : query.getPipeline() ) {
            if ( !value.isDocument() || value.asDocument().size() != 1 ) {
                throw new RuntimeException( "Each initial update steps document in the aggregate pipeline can only have one key." );
            }
            String key = value.asDocument().getFirstKey();
            if ( !value.asDocument().get( key ).isDocument() ) {
                throw new RuntimeException( "The update document needs one key and a document." );
            }
            BsonDocument doc = value.asDocument().get( key ).asDocument();
            switch ( key ) {
                case "$addFields":
                case "$set":
                    updates.putAll( translateAddToSet( doc, rowType ) );
                    updateOp = UpdateOperation.REPLACE;
                    break;
                case "$project":
                case "$unset":
                    updates.putAll( translateUnset( doc, rowType ) );
                    updateOp = UpdateOperation.REMOVE;
                    break;
                default:
                    throw new RuntimeException( "The used statement is not supported in the update aggregation pipeline" );
            }

            mergeUpdates( mergedUpdates, rowType, updates, updateOp );
            updates.clear();

        }
        return finalizeUpdates( "_data", mergedUpdates, rowType, node, table );

    }


    /**
     * Translates a delete operation from its MqlNode format to the {@link AlgNode} form
     */
    private AlgNode convertDelete( MqlDelete query, AlgOptTable table, AlgNode node ) {
        if ( !query.getQuery().isEmpty() ) {
            node = convertQuery( query, table.getRowType(), node );
        }
        if ( query.isOnlyOne() ) {
            node = wrapLimit( node, 1 );
        }

        return LogicalDocumentModify.create(
                table,
                node,
                catalogReader, Operation.DELETE,
                null,
                null );
    }


    /**
     * Method transforms an insert into the appropriate {@link LogicalDocumentValues}
     *
     * @param query the insert statement as Mql object
     * @param table the table/collection into which the values are inserted
     * @return the modified AlgNode
     */
    private AlgNode convertInsert( MqlInsert query, AlgOptTable table ) {
        return LogicalDocumentModify.create(
                table,
                convertMultipleValues( query.getValues(), table.getRowType() ),
                catalogReader, Operation.INSERT,
                null,
                null );
    }


    private AlgNode convertCount( MqlCount query, AlgDataType rowType, AlgNode node ) {
        node = convertQuery( query, rowType, node );

        return LogicalDocumentAggregate.create(
                node,
                ImmutableBitSet.of(),
                Collections.singletonList( ImmutableBitSet.of() ),
                Collections.singletonList(
                        AggregateCall.create(
                                OperatorRegistry.getAgg( OperatorName.COUNT ),
                                false,
                                query.isEstimate(),
                                new ArrayList<>(),
                                -1,
                                AlgCollations.EMPTY,
                                cluster.getTypeFactory().createPolyType( PolyType.BIGINT ),
                                query.isEstimate() ? "estimatedCount" : "count"
                        ) ) );
    }


    /**
     * To correctly represent the values according to the used model they have to be inserted into their {@link Values}
     * representation
     *
     * @param array the values, which are inserted
     * @param rowType row definition, which is used to determine fixed columns
     * @return the {@link Values} representation of the values
     */
    private AlgNode convertMultipleValues( BsonArray array, AlgDataType rowType ) {
        LogicalDocumentValues docs = (LogicalDocumentValues) LogicalDocumentValues.create( cluster, ImmutableList.copyOf( array.asArray() ) );
        if ( usesDocumentModel ) {
            return docs;
        } else {
            return docs.getRelationalEquivalent();
        }

    }


    /**
     * Returns a RelDataType for the given BsonValue
     *
     * @param value the untransformed BSON
     */
    private AlgDataType getRelDataType( BsonValue value ) {
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


    private RexCall createJsonify( RexNode ref ) {
        return new RexCall( any, OperatorRegistry.get( QueryLanguage.MONGO_QL, OperatorName.MQL_JSONIFY ), Collections.singletonList( ref ) );
    }


    /**
     * Starts converting of aggregation pipeline
     *
     * Example:
     * <pre>
     * db.collection.aggregate([
     *      {"$project": {"key":1}}, // {@code ->} {@link #combineProjection(BsonValue, AlgNode, AlgDataType, boolean, boolean)}
     *      {"$match": {"key.subkey": "test"}} // {@code ->} {@link #combineFilter(BsonDocument, AlgNode, AlgDataType)}
     * ])
     * </pre>
     */
    private AlgNode convertAggregate( MqlAggregate query, AlgDataType rowType, AlgNode node ) {
        this.excludedId = false;

        for ( BsonValue value : query.getPipeline() ) {
            if ( !value.isDocument() && ((BsonDocument) value).size() > 1 ) {
                throw new RuntimeException( "The aggregation pipeline is not used correctly." );
            }
            switch ( ((BsonDocument) value).getFirstKey() ) {
                case "$match":
                    node = combineFilter( value.asDocument().getDocument( "$match" ), node, rowType );
                    node.getRowType();
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
                    node.getRowType();
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
                rowType = node.getRowType();
            }
        }

        AlgNode finalNode = node;
        node = LogicalDocumentProject.create( node,
                node.getRowType().getFieldList()
                        .stream()
                        .map( el -> {
                            RexInputRef ref = new RexInputRef( el.getIndex(), finalNode.getRowType().getFieldList().get( el.getIndex() ).getType() );
                            if ( !el.getName().equals( "_id" ) ) {
                                return createJsonify( ref );
                            } else {
                                return ref;
                            }
                        } )
                        .collect( Collectors.toList() ), node.getRowType().getFieldNames() );
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
                throw new RuntimeException( "$replaceRoot requires a document." );
            }
            BsonDocument doc = value.asDocument();
            if ( !doc.containsKey( "newRoot" ) ) {
                throw new RuntimeException( "$replaceRoot requires a document with the key 'newRoot'" );
            }
            newRoot = doc.get( "newRoot" );
        }

        if ( !newRoot.isDocument() && !newRoot.isString() ) {
            throw new RuntimeException( "The used root for $replaceRoot needs to either be a string or a document" );
        }

        DocumentProject project;
        if ( newRoot.isDocument() ) {
            project = LogicalDocumentProject.create(
                    node,
                    Collections.singletonList( translateDocument( newRoot.asDocument(), node.getRowType(), null ) ),
                    Collections.singletonList( "_data" )
            );
        } else {
            if ( !newRoot.asString().getValue().startsWith( "$" ) ) {
                throw new RuntimeException( "The used root needs to be a reference to a field" );
            }

            project = LogicalDocumentProject.create(
                    node,
                    Collections.singletonList( getIdentifier( newRoot.asString().getValue().substring( 1 ), node.getRowType() ) ),
                    Collections.singletonList( "_data" )
            );
        }
        _dataExists = false;
        return project;
    }


    /**
     * Transforms the $unwind stage in the aggregation pipeline
     * this operation unfolds a specified array into multiple records
     *
     * {"test","key",[3,1,"te"]} {@literal ->} {"test","key",3},{"test","key",1},{"test","key","te"}
     *
     * @param value the unparsed $unwind operation
     */
    private AlgNode combineUnwind( BsonValue value, AlgNode node ) {
        if ( !value.isString() && !value.isDocument() ) {
            throw new RuntimeException( "$unwind pipeline stage needs either a document or a string describing the path." );
        }

        String path;
        if ( value.isString() ) {
            path = value.asString().getValue();
        } else {
            if ( !value.asDocument().containsKey( "path" ) && value.asDocument().get( "path" ).isString() ) {
                throw new RuntimeException( "The used document in the $unwind stage needs the key \"path\" with a string value." );
            }
            path = value.asDocument().get( "path" ).asString().getValue();
        }
        if ( !path.startsWith( "$" ) ) {
            throw new RuntimeException( "$unwind pipeline stage needs either a document or a string describing the path, which is prefixed with \"$\"." );
        }
        path = path.substring( 1 );

        RexNode id = getIdentifier( path, node.getRowType() );

        RexCall call = new RexCall( any, OperatorRegistry.get( QueryLanguage.MONGO_QL, OperatorName.MQL_UNWIND ), Collections.singletonList( id ) );

        List<String> names = new ArrayList<>();
        List<RexNode> values = new ArrayList<>();

        String firstKey = path.split( "\\." )[0];

        if ( node.getRowType().getFieldNames().contains( firstKey ) ) {
            for ( AlgDataTypeField field : node.getRowType().getFieldList() ) {
                if ( !field.getName().equals( firstKey ) ) {
                    names.add( field.getName() );
                    values.add( getIdentifier( field.getName(), node.getRowType() ) );
                }
            }
            names.add( firstKey );
            values.add( call );
        } else {
            for ( AlgDataTypeField field : node.getRowType().getFieldList() ) {
                if ( !field.getName().equals( "_data" ) ) {
                    names.add( field.getName() );
                    values.add( getIdentifier( field.getName(), node.getRowType() ) );
                }
            }
            names.add( "_data" );
            values.add( call );
        }

        return LogicalDocumentProject.create( node, values, names );
    }


    /**
     * Translates a $skip stage of the aggregation pipeline
     * <pre>
     *     { $skip: <positive 64-bit integer> }
     * </pre>
     *
     * @param value the untransformed BSON value
     * @param node the node up to this point
     * @return the provided node with the applied skip stage
     */
    private AlgNode combineSkip( BsonValue value, AlgNode node ) {
        if ( !value.isNumber() || value.asNumber().intValue() < 0 ) {
            throw new RuntimeException( "$skip pipeline stage needs a positive number after" );
        }

        return LogicalDocumentSort.create( node, AlgCollations.of(), convertLiteral( value ), null );
    }


    /**
     * Translates a $skip stage of the aggregation pipeline
     * <pre>
     *     { $limit: <positive 64-bit integer> }
     * </pre>
     *
     * @param value the untransformed BSON value
     * @param node the node up to this point
     * @return the provided node with the applied limit stage
     */
    private AlgNode combineLimit( BsonValue value, AlgNode node ) {
        if ( !value.isNumber() || value.asNumber().intValue() < 0 ) {
            throw new RuntimeException( "$limit pipeline stage needs a positive number after" );
        }

        return LogicalDocumentSort.create( node, AlgCollations.of(), null, convertLiteral( value ) );
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
            throw new RuntimeException( "$sort pipeline stage needs a document after" );
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
                ( newNode, newRowType ) -> LogicalDocumentSort.create( newNode, AlgCollations.of( generateCollation( dirs, names, newRowType.getFieldNames() ) ), null, null ) );
    }


    /**
     * This function wraps document fields which are either hidden in the default data field _data or in another parent field
     */
    private AlgNode conditionalWrap( AlgNode node, AlgDataType rowType, List<String> names, BiFunction<AlgNode, AlgDataType, AlgNode> nodeFunction ) {
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

        // those underlying field need to be added via a projection, then the alg operation applied and then removed again
        if ( hiddenNames.size() > 0 ) {

            List<RexNode> nodes = rowType.getFieldList().stream().map( f -> RexInputRef.of( f.getIndex(), rowType ) ).collect( Collectors.toList() );
            nodes.addAll( projectionNodes );
            List<String> nodeNames = rowType.getFieldList().stream().map( AlgDataTypeField::getName ).collect( Collectors.toList() );
            nodeNames.addAll( hiddenNames );
            node = LogicalDocumentProject.create( node, nodes, nodeNames );

            node = nodeFunction.apply( node, node.getRowType() );

            nodes.removeAll( projectionNodes );
            nodeNames.removeAll( hiddenNames );

            return LogicalDocumentProject.create( node, nodes, nodeNames );
        }
        return nodeFunction.apply( node, node.getRowType() );
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
            throw new RuntimeException( "$group pipeline stage needs a document after, which defines a _id" );
        }

        List<AggFunction> ops = new ArrayList<>();
        List<RexNode> nodes = new ArrayList<>();
        List<String> names = new ArrayList<>();
        List<String> aggNames = new ArrayList<>();

        for ( Entry<String, BsonValue> entry : value.asDocument().entrySet() ) {
            if ( entry.getKey().equals( "_id" ) ) {
                if ( entry.getValue().isNull() ) {
                    names.addAll( 0, rowType.getFieldNames() );
                    nodes.addAll( 0, rowType.getFieldList().stream().map( f -> RexInputRef.of( f.getIndex(), rowType ) ).collect( Collectors.toList() ) );

                } else if ( entry.getValue().isString() ) {
                    names.add( entry.getValue().asString().getValue().substring( 1 ) );
                    nodes.add( getIdentifier( entry.getValue().asString().getValue().substring( 1 ), rowType ) );
                } else if ( entry.getValue().isDocument() ) {
                    for ( Entry<String, BsonValue> idEntry : entry.getValue().asDocument().entrySet() ) {
                        names.add( idEntry.getValue().asString().getValue().substring( 1 ) );
                        nodes.add( getIdentifier( idEntry.getValue().asString().getValue().substring( 1 ), rowType ) );
                    }
                } else {
                    throw new RuntimeException( "$group takes as _id values either a document or a string" );
                }
            } else {
                if ( !entry.getValue().isDocument() ) {
                    throw new RuntimeException( "$group needs a document with an accumulator and an expression" );
                }
                BsonDocument doc = entry.getValue().asDocument();
                ops.add( accumulators.get( doc.getFirstKey() ) );
                aggNames.add( entry.getKey() );
                names.add( entry.getKey() );
                AlgDataType nullableDouble = cluster.getTypeFactory().createTypeWithNullability( cluster.getTypeFactory().createPolyType( PolyType.DOUBLE ), true );
                // when using aggregations MongoQl automatically casts to doubles
                nodes.add( cluster.getRexBuilder().makeAbstractCast(
                        nullableDouble,
                        convertExpression( doc.get( doc.getFirstKey() ), rowType ) ) );
            }
        }

        node = LogicalDocumentProject.create( node, nodes, names );

        return groupBy( value, node, node.getRowType(), aggNames, ops );
    }


    private RexNode convertExpression( BsonValue value, AlgDataType rowType ) {
        if ( value.isDocument() ) {
            BsonDocument doc = value.asDocument();
            if ( mathOperators.containsKey( doc.getFirstKey() ) ) {
                return convertMath( doc.getFirstKey(), null, doc.get( doc.getFirstKey() ), rowType, false );
            } else {
                return convertSingleMath( doc.getFirstKey(), doc.get( doc.getFirstKey() ), rowType );
            }

        } else if ( value.isString() ) {
            return getIdentifier( value.asString().getValue().substring( 1 ), rowType );
        } else if ( value.isNumber() ) {
            return convertLiteral( value );
        }
        return null;
    }


    private AlgNode groupBy( BsonValue value, AlgNode node, AlgDataType rowType, List<String> names, List<AggFunction> aggs ) {
        BsonValue groupBy = value.asDocument().get( "_id" );

        List<AggregateCall> convertedAggs = new ArrayList<>();
        int pos = 0;
        for ( String name : names ) {

            convertedAggs.add(
                    AggregateCall.create(
                            aggs.get( pos ),
                            false,
                            false,
                            Collections.singletonList( rowType.getFieldNames().indexOf( name ) ),
                            -1,
                            AlgCollations.EMPTY,
                            // when using aggregations MongoQl automatically casts to doubles
                            cluster.getTypeFactory().createTypeWithNullability( cluster.getTypeFactory().createPolyType( PolyType.DOUBLE ), true ),
                            name ) );
            pos++;
        }

        if ( !groupBy.isNull() ) {
            String groupName = groupBy.asString().getValue().substring( 1 );
            int index = rowType.getFieldNames().indexOf( groupName );

            node = LogicalDocumentAggregate.create(
                    node,
                    ImmutableBitSet.of( index ),
                    Collections.singletonList( ImmutableBitSet.of( index ) ),
                    convertedAggs );
        } else {

            node = LogicalDocumentAggregate.create(
                    node,
                    ImmutableBitSet.of(),
                    Collections.singletonList( ImmutableBitSet.of() ),
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
            throw new RuntimeException( "$count pipeline stage needs only a string" );
        }
        return LogicalDocumentAggregate.create(
                node,
                ImmutableBitSet.of(),
                Collections.singletonList( ImmutableBitSet.of() ),
                Collections.singletonList(
                        AggregateCall.create(
                                OperatorRegistry.getAgg( OperatorName.COUNT ),
                                false,
                                false,
                                new ArrayList<>(),
                                -1,
                                AlgCollations.EMPTY,
                                cluster.getTypeFactory().createPolyType( PolyType.BIGINT ),
                                value.asString().getValue()
                        ) ) );
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
                null,
                new RexLiteral(
                        new BigDecimal( limit ),
                        cluster.getTypeFactory()
                                .createPolyType( PolyType.INTEGER ), PolyType.DECIMAL )
        );
    }


    private AlgNode combineFilter( BsonDocument filter, AlgNode node, AlgDataType rowType ) {
        RexNode condition = translateDocument( filter, rowType, null );

        return LogicalDocumentFilter.create( node, condition );
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
                        return new RexCall( type, OperatorRegistry.get( QueryLanguage.MONGO_QL, OperatorName.MQL_EQUALS ), Arrays.asList( id, node ) );
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
            }  // handle others
        }
        return getFixedCall( operands, OperatorRegistry.get( OperatorName.AND ), PolyType.BOOLEAN );
    }


    private RexNode convertMod( RexNode id, BsonValue bsonValue, AlgDataType rowType ) {
        String msg = "$mod requires an array of two values or documents.";
        if ( !bsonValue.isArray() && bsonValue.asArray().size() != 2 ) {
            throw new RuntimeException( msg );
        }

        BsonValue divider = bsonValue.asArray().get( 0 );
        RexNode rexDiv;
        if ( divider.isDocument() ) {
            rexDiv = translateDocument( divider.asDocument(), rowType, null );
        } else if ( divider.isNumber() ) {
            rexDiv = convertLiteral( divider );
        } else {
            throw new RuntimeException( msg );
        }

        BsonValue remainder = bsonValue.asArray().get( 1 );
        RexNode rexRemainder;
        if ( remainder.isDocument() ) {
            rexRemainder = translateDocument( remainder.asDocument(), rowType, null );
        } else if ( remainder.isNumber() ) {
            rexRemainder = convertLiteral( remainder );
        } else {
            throw new RuntimeException( msg );
        }

        RexNode node = new RexCall(
                this.cluster.getTypeFactory().createPolyType( PolyType.INTEGER ),
                OperatorRegistry.get( OperatorName.MOD ),
                Arrays.asList(
                        this.builder.makeCast( cluster.getTypeFactory().createPolyType( PolyType.INTEGER ), id ),
                        rexDiv ) );

        AlgDataType type = cluster.getTypeFactory().createPolyType( PolyType.BOOLEAN );

        return new RexCall( type, OperatorRegistry.get( QueryLanguage.MONGO_QL, OperatorName.MQL_EQUALS ), Arrays.asList( node, rexRemainder ) );
    }


    private AlgDataTypeField getDefaultDataField( AlgDataType rowType ) {
        return rowType.getField( "d", false, false );
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
            throw new RuntimeException( "After a $slice projection a number or an array of 2 is needed" );
        }

        return new RexCall( any, OperatorRegistry.get( QueryLanguage.MONGO_QL, OperatorName.MQL_SLICE ), Arrays.asList( id, convertLiteral( skip ), convertLiteral( elements ) ) );
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
                throw new RuntimeException( msg );
            }
            return new RexCall( any, OperatorRegistry.get( QueryLanguage.MONGO_QL, OperatorName.MQL_ITEM ), Arrays.asList( nodes.get( 0 ), nodes.get( 1 ) ) );

        } else {
            throw new RuntimeException( msg );
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
            throw new RuntimeException( errorMsg );
        }
    }


    private RexNode convertSingleMath( String key, BsonValue value, AlgDataType rowType ) {
        Operator op = singleMathOperators.get( key );
        if ( value.isArray() ) {
            throw new RuntimeException( "The " + key + " operator needs either a single expression or a document." );
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
                    throw new RuntimeException( "$logical operations inside $not operations are not possible." );
                }
                op = OperatorRegistry.get( OperatorName.AND );
                return convertLogicalArray( parentKey, bsonValue, rowType, op, false );
            case "$or":
                if ( notActive ) {
                    throw new RuntimeException( "$logical operations inside $not operations are not possible." );
                }
                op = OperatorRegistry.get( OperatorName.OR );
                return convertLogicalArray( parentKey, bsonValue, rowType, op, false );
            case "$nor":
                if ( notActive ) {
                    throw new RuntimeException( "$logical operations inside $not operations are not possible." );
                }
                op = OperatorRegistry.get( OperatorName.AND );
                return convertLogicalArray( parentKey, bsonValue, rowType, op, true );
            case "$not":
                this.notActive = true;
                op = OperatorRegistry.get( OperatorName.NOT );
                if ( bsonValue.isDocument() ) {
                    AlgDataType type = cluster.getTypeFactory().createPolyType( PolyType.BOOLEAN );
                    return new RexCall( type, op, Collections.singletonList( translateDocument( bsonValue.asDocument(), rowType, parentKey ) ) );
                } else {
                    throw new RuntimeException( "After a $not a document is needed" );
                }

            default:
                throw new RuntimeException( "This logical operator was not recognized:" );
        }
    }


    private RexNode convertLogicalArray( String parentKey, BsonValue bsonValue, AlgDataType rowType, Operator op, boolean isNegated ) {
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
                throw new RuntimeException( errorMsg );
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
                nodes.add( getArray(
                        arr,
                        cluster.getTypeFactory().createArrayType( nullableAny, arr.size() ) ) );
            } else if ( bsonValue.isRegularExpression() ) {
                return convertRegex( bsonValue, parentKey, rowType );
            } else {
                nodes.add( convertLiteral( bsonValue ) );
            }

            return getFixedCall( nodes, OperatorRegistry.get( QueryLanguage.MONGO_QL, OperatorName.MQL_EQUALS ), PolyType.BOOLEAN );
        }
    }


    private RexNode getIdentifier( String parentKey, AlgDataType rowType ) {
        return getIdentifier( parentKey, rowType, false );
    }


    private RexNode getIdentifier( String parentKey, AlgDataType rowType, boolean useAccess ) {
        List<String> rowNames = rowType.getFieldNames();
        if ( rowNames.contains( parentKey ) ) {
            if ( useAccess ) {
                return attachAccess( parentKey, rowType );
            } else {
                return attachRef( parentKey, rowType );
            }
        }
        // as it is possible to query relational schema with mql we have to block this step when this happens
        if ( !usesDocumentModel ) {
            throw new RuntimeException( "The used identifier is not part of the table." );
        }

        if ( rowNames.contains( parentKey.split( "\\." )[0] ) ) {
            String[] keys = parentKey.split( "\\." );
            // we fix sub-documents in elemMatch queries by only passing the sub-key
            // that the replacing then only uses the array element and searches the sub-key there
            if ( !elemMatchActive || keys.length == 1 ) {
                return translateJsonValue( rowNames.indexOf( keys[0] ), rowType, parentKey, useAccess );
            } else {
                List<String> names = Arrays.asList( keys );
                names.remove( 0 );
                return translateJsonValue( rowNames.indexOf( keys[0] ), rowType, String.join( ".", names ), useAccess );
            }

        } else if ( _dataExists ) {
            // the default _data field does still exist
            return translateJsonValue( getDefaultDataField( rowType ).getIndex(), rowType, parentKey, useAccess );
        } else {
            return null;
        }
    }


    private RexNode attachRef( String parentKey, AlgDataType rowType ) {
        AlgDataTypeField field = rowType.getField( parentKey, false, false );
        return RexInputRef.of( field.getIndex(), rowType );
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
            if ( toAdd.size() > 0 ) {
                operands.addAll( toAdd );
                operands.removeAll( toRemove );
            }

            return new RexCall( cluster.getTypeFactory().createTypeWithNullability( cluster.getTypeFactory().createPolyType( polyType ), true ), op, operands );
        }
    }


    private RexNode translateDocument( BsonDocument bsonDocument, AlgDataType rowType, String parentKey ) {
        ArrayList<RexNode> operands = new ArrayList<>();

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
            throw new RuntimeException( "$regex either needs to be either a document or a regular expression." );
        }

        String stringRegex;
        if ( regex.isString() ) {
            stringRegex = regex.asString().getValue();
        } else if ( regex.isRegularExpression() ) {
            BsonRegularExpression regbson = regex.asRegularExpression();
            stringRegex = regbson.getPattern();
            options += regbson.getOptions();
        } else {
            throw new RuntimeException( "$regex needs to be either a regular expression or a string" );
        }

        return getRegex( stringRegex, options, parentKey, rowType );
    }


    private RexCall getRegex( String stringRegex, String options, String parentKey, AlgDataType rowType ) {
        return new RexCall(
                cluster.getTypeFactory().createPolyType( PolyType.BOOLEAN ),
                OperatorRegistry.get( QueryLanguage.MONGO_QL, OperatorName.MQL_REGEX_MATCH ),
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
        if ( value.isBoolean() ) {
            List<String> keys = Arrays.asList( parentKey.split( "\\." ) );

            String key = keys.get( 0 );
            if ( !rowType.getFieldNames().contains( key ) ) {
                key = getDefaultDataField( rowType ).getName();
            } else {
                keys = keys.subList( 1, keys.size() );
            }

            RexCall exists = new RexCall(
                    cluster.getTypeFactory().createPolyType( PolyType.BOOLEAN ),
                    OperatorRegistry.get( QueryLanguage.MONGO_QL, OperatorName.MQL_EXISTS ),
                    Arrays.asList( getIdentifier( key, rowType ), getStringArray( keys ) ) );

            if ( !value.asBoolean().getValue() ) {
                return negate( exists );
            }
            return exists;

        } else {
            throw new RuntimeException( "$exists without a boolean is not supported" );
        }
    }


    private int getIndexOfParentField( String parentKey, AlgDataType rowType ) {
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


    private RexNode convertExpr( BsonValue bsonValue, String parentKey, AlgDataType rowType ) {
        if ( bsonValue.isDocument() && bsonValue.asDocument().size() == 1 ) {
            BsonDocument doc = bsonValue.asDocument();
            return convertMath( doc.getFirstKey(), parentKey, doc.get( doc.getFirstKey() ), rowType, true );

        } else {
            throw new RuntimeException( "After $expr there needs to be a document with a single entry" );
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
            return new RexCall( nullableAny, OperatorRegistry.get( QueryLanguage.MONGO_QL, OperatorName.MQL_JSON_MATCH ), Collections.singletonList( RexInputRef.of( getIndexOfParentField( "d", rowType ), rowType ) ) );
        } else {
            throw new RuntimeException( "After $jsonSchema there needs to follow a document" );
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
            return new RexCall( cluster.getTypeFactory().createPolyType( PolyType.BOOLEAN ), OperatorRegistry.get( QueryLanguage.MONGO_QL, OperatorName.MQL_SIZE_MATCH ), Arrays.asList( id, convertLiteral( bsonValue ) ) );
        } else {
            throw new RuntimeException( "After $size there needs to follow a number" );
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
            throw new RuntimeException( "After $elemMatch there needs to follow a document" );
        }
        this.elemMatchActive = true;
        List<RexNode> nodes = new ArrayList<>();
        for ( Entry<String, BsonValue> entry : bsonValue.asDocument().entrySet() ) {
            nodes.add( convertEntry( entry.getKey(), parentKey, entry.getValue(), rowType ) );
        }
        this.elemMatchActive = false;

        RexNode op = getFixedCall( nodes, OperatorRegistry.get( OperatorName.AND ), PolyType.BOOLEAN );

        return new RexCall( cluster.getTypeFactory().createPolyType( PolyType.BOOLEAN ), OperatorRegistry.get( QueryLanguage.MONGO_QL, OperatorName.MQL_ELEM_MATCH ), Arrays.asList( getIdentifier( parentKey, rowType ), op ) );
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
                operands.add( new RexCall( type, OperatorRegistry.get( QueryLanguage.MONGO_QL, OperatorName.MQL_EQUALS ), Arrays.asList( id, rexNode ) ) );
            }

            return getFixedCall( operands, OperatorRegistry.get( OperatorName.AND ), PolyType.BOOLEAN );
        } else {
            throw new RuntimeException( "After $all there needs to follow a array" );
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
                    throw new RuntimeException( errorMsg );
                }
            }
            types = getIntArray( DocumentUtil.removePlaceholderTypes( numbers ) );
        } else if ( value.isNumber() || value.isString() ) {
            int typeNumber = value.isNumber() ? value.asNumber().intValue() : DocumentUtil.getTypeNumber( value.asString().getValue() );
            types = getIntArray( DocumentUtil.removePlaceholderTypes( Collections.singletonList( typeNumber ) ) );
        } else {
            throw new RuntimeException( errorMsg );
        }
        return new RexCall(
                cluster.getTypeFactory().createPolyType( PolyType.BOOLEAN ),
                OperatorRegistry.get( QueryLanguage.MONGO_QL, OperatorName.MQL_TYPE_MATCH ),
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
     * @param rowType the row information of the collection/table
     * @param key the key in the form "key2.key22"
     * @param useAccess if access or input operations should be used
     * @return the node, which defines the retrieval of the underlying key
     */
    private RexNode translateJsonValue( int index, AlgDataType rowType, String key, boolean useAccess ) {
        RexCall filter;
        List<String> names = Arrays.asList( key.split( "\\." ) );
        if ( elemMatchActive ) {
            names = names.subList( 1, names.size() );
        }
        filter = getStringArray( names );

        return new RexCall(
                any,
                OperatorRegistry.get( QueryLanguage.MONGO_QL, OperatorName.MQL_QUERY_VALUE ),
                Arrays.asList(
                        /*useAccess
                                ? attachAccess( index, rowType )
                                : */RexInputRef.of( index, rowType ),
                        filter ) );
    }


    private RexNode translateJsonQuery( int index, AlgDataType rowType, List<String> excludes ) {
        RexCall filter = getNestedArray( excludes.stream().map( e -> Arrays.asList( e.split( "\\." ) ) ).collect( Collectors.toList() ) );
        return new RexCall( any, OperatorRegistry.get( QueryLanguage.MONGO_QL, OperatorName.MQL_EXCLUDE ), Arrays.asList( RexInputRef.of( index, rowType ), filter ) );
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
                OperatorRegistry.get( OperatorName.ARRAY_VALUE_CONSTRUCTOR ), nodes );
    }


    private RexCall getIntArray( List<Integer> elements ) {
        List<RexNode> rexNodes = new ArrayList<>();
        for ( Integer name : elements ) {
            rexNodes.add( convertLiteral( new BsonInt32( name ) ) );
        }

        AlgDataType type = cluster.getTypeFactory().createArrayType(
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

        AlgDataType type = cluster.getTypeFactory().createArrayType(
                cluster.getTypeFactory().createPolyType( PolyType.CHAR, maxSize ),
                rexNodes.size() );
        return getArray( rexNodes, type );
    }


    private RexCall getArray( List<RexNode> elements, AlgDataType type ) {
        return new RexCall( type, OperatorRegistry.get( OperatorName.ARRAY_VALUE_CONSTRUCTOR ), elements );
    }


    private RexNode convertLiteral( BsonValue bsonValue ) {
        AlgDataType type = getRelDataType( bsonValue );
        if ( bsonValue.isArray() ) {
            List<RexNode> arr = bsonValue.asArray().stream().map( this::convertLiteral ).collect( Collectors.toList() );
            return getArray( arr, any );
        } else {
            Pair<Comparable, PolyType> valuePair = RexLiteral.convertType( getComparable( bsonValue ), type );
            return new RexLiteral( valuePair.left, type, valuePair.right );
        }
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
                throw new RuntimeException( "Non-literal in $in clauses are not supported" );
            }
            if ( literal.isRegularExpression() ) {
                RexNode filter = getRegex( literal.asRegularExpression().getPattern(), literal.asRegularExpression().getOptions(), key, rowType );
                if ( !isIn ) {
                    filter = negate( filter );
                }
                operands.add( filter );
            } else {
                operands.add( new RexCall( type, isIn ? OperatorRegistry.get( QueryLanguage.MONGO_QL, OperatorName.MQL_EQUALS ) : OperatorRegistry.get( OperatorName.NOT_EQUALS ), Arrays.asList( id, convertLiteral( literal ) ) ) );
            }
        }

        return getFixedCall( operands, op, PolyType.BOOLEAN );
    }


    /**
     * Helper method, which returns a PolyType for a BsonValue
     */
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
        List<String> includesOrder = new ArrayList<>();

        BsonDocument projection;
        if ( projectionValue.isDocument() ) {
            projection = projectionValue.asDocument();
            translateProjection( rowType, isAddFields, isUnset, includes, excludes, includesOrder, projection );
        } else if ( projectionValue.isArray() || projectionValue.isString() ) {
            List<BsonValue> array;
            if ( projectionValue.isArray() ) {
                array = projectionValue.asArray();
            } else if ( projectionValue.isString() ) {
                array = new BsonArray( Collections.singletonList( projectionValue.asString() ) );
            } else {
                throw new RuntimeException( "$unset or $addFields needs a string or an array of strings" );
            }

            for ( BsonValue value : array ) {
                if ( isUnset ) {
                    excludes.add( value.asString().getValue() );
                } else {
                    includes.put( value.asString().getValue(), getIdentifier( value.asString().getValue(), rowType ) );
                    includesOrder.add( value.asString().getValue() );
                }
            }


        } else {
            throw new RuntimeException( "The provided projection was not translatable" );
        }

        if ( includes.size() != 0 && excludes.size() != 0 ) {
            throw new RuntimeException( "Include projection and exclude projections are not possible at the same time." );
        }

        if ( excludes.size() > 0 ) {
            if ( _dataExists ) {
                // we have still the _data field, where the fields need to be extracted
                // exclusion projections only work for the underlying _data field
                AlgDataTypeField defaultDataField = getDefaultDataField( rowType );

                List<RexNode> values = new ArrayList<>( Collections.singletonList( translateJsonQuery( defaultDataField.getIndex(), rowType, excludes ) ) );
                List<String> names = new ArrayList<>( Collections.singletonList( defaultDataField.getName() ) );

                // we only need to do this if it is the second time
                /*if ( !excludes.contains( "_id" ) && !excludedId ) {
                    names.add( 0, "_id" );
                    values.add( 0, RexInputRef.of( 0, rowType ) );
                }

                if ( excludes.contains( "_id" ) ) {
                    this.excludedId = true;
                }*/

                return LogicalDocumentProject.create( node, values, names );
            } else {
                // we already projected the _data field away and have to work with what we got
                List<RexNode> values = new ArrayList<>();
                List<String> names = new ArrayList<>();

                for ( AlgDataTypeField field : rowType.getFieldList() ) {
                    if ( !excludes.contains( field.getName() ) ) {
                        names.add( field.getName() );
                        values.add( RexInputRef.of( field.getIndex(), rowType ) );
                    }
                }

                return LogicalDocumentProject.create( node, values, names );
            }
        } else if ( isAddFields && _dataExists ) {
            List<String> names = rowType.getFieldNames();

            // we have to implement the added fields into the _data field
            // as this is later used to retrieve them when projecting

            int dataIndex = rowType.getFieldNames().indexOf( "d" );

            for ( Entry<String, RexNode> entry : includes.entrySet() ) {
                List<RexNode> values = new ArrayList<>();
                for ( AlgDataTypeField field : rowType.getFieldList() ) {
                    if ( field.getIndex() != dataIndex ) {
                        values.add( RexInputRef.of( field.getIndex(), rowType ) );
                    } else {
                        // we attach the new values to the input bson
                        values.add( new RexCall(
                                any,
                                OperatorRegistry.get( QueryLanguage.MONGO_QL, OperatorName.MQL_ADD_FIELDS ),
                                Arrays.asList(
                                        RexInputRef.of( dataIndex, rowType ),
                                        convertLiteral( new BsonString( entry.getKey() ) ),
                                        entry.getValue() ) ) );
                    }
                }

                node = LogicalDocumentProject.create( node, values, names );
            }

            return node;
        } else if ( includes.size() > 0 ) {
            List<RexNode> values = includesOrder.stream().map( includes::get ).collect( Collectors.toList() );

            /*if ( !includes.containsKey( "_id" ) && !excludedId ) {
                includesOrder.add( 0, "_id" );
                values.add( 0, RexInputRef.of( 0, rowType ) );
            }*/

            if ( isAddFields ) {
                for ( AlgDataTypeField field : rowType.getFieldList() ) {
                    if ( !includesOrder.contains( field.getName() ) ) {
                        includesOrder.add( field.getName() );
                        values.add( RexInputRef.of( field.getIndex(), rowType ) );
                    }
                }
            }

            // the _data field does not longer exist, as we made a projection "out" of it
            this._dataExists = false;

            return LogicalDocumentProject.create( node, values, includesOrder );
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
     * @param includesOrder order collection to assure same projection order on retrieval
     * @param projection document, which defines the structure of the projection
     */
    private void translateProjection( AlgDataType rowType, boolean isAddFields, boolean isUnset, Map<String, RexNode> includes, List<String> excludes, List<String> includesOrder, BsonDocument projection ) {
        for ( Entry<String, BsonValue> entry : projection.entrySet() ) {
            BsonValue value = entry.getValue();
            if ( value.isNumber() && !isAddFields ) {
                // we have a simple projection; [name]: 1 (include) or [name]:0 (exclude)
                if ( value.asNumber().intValue() == 1 ) {
                    includes.put( entry.getKey(), getIdentifier( entry.getKey(), rowType ) );
                    includesOrder.add( entry.getKey() );
                } else if ( value.asNumber().intValue() == 0 ) {
                    excludes.add( entry.getKey() );
                }

            } else if ( value.isString() && value.asString().getValue().startsWith( "$" ) ) {
                // we have a renaming; [new name]: $[old name] ( this counts as a inclusion projection
                String oldName = value.asString().getValue().substring( 1 );

                includes.put( entry.getKey(), getIdentifier( oldName, rowType ) );
                includesOrder.add( entry.getKey() );
            } else if ( value.isDocument() && value.asDocument().size() == 1 && value.asDocument().getFirstKey().startsWith( "$" ) ) {
                String func = value.asDocument().getFirstKey();
                AlgDataTypeField field = getDefaultDataField( rowType );
                if ( mathOperators.containsKey( func ) ) {
                    includes.put( entry.getKey(), convertMath( func, entry.getKey(), value.asDocument().get( func ), rowType, false ) );
                    includesOrder.add( entry.getKey() );
                } else if ( func.equals( "$arrayElemAt" ) ) {
                    includes.put( entry.getKey(), convertArrayAt( entry.getKey(), value.asDocument().get( func ), rowType ) );
                    includesOrder.add( entry.getKey() );
                } else if ( func.equals( "$slice" ) ) {
                    includes.put( entry.getKey(), convertSlice( entry.getKey(), value.asDocument().get( func ), rowType ) );
                    includesOrder.add( entry.getKey() );
                } else if ( func.equals( "$literal" ) ) {
                    if ( value.asDocument().get( func ).isInt32() && value.asDocument().get( func ).asInt32().getValue() == 0 ) {
                        excludes.add( entry.getKey() );
                    } else {
                        includes.put( entry.getKey(), getIdentifier( entry.getKey(), rowType ) );
                        includesOrder.add( entry.getKey() );
                    }
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
                includesOrder.add( entry.getKey() );
            } else if ( isUnset && value.isArray() ) {
                for ( BsonValue bsonValue : value.asArray() ) {
                    if ( bsonValue.isString() ) {
                        excludes.add( bsonValue.asString().getValue() );
                    } else {
                        throw new RuntimeException( "When using $unset with an array, it can only contain strings" );
                    }
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
    }


    /**
     * Defines one of the possible doc update operations
     */
    enum UpdateOperation {
        RENAME( OperatorRegistry.get( QueryLanguage.MONGO_QL, OperatorName.MQL_UPDATE_RENAME ) ),
        REPLACE( OperatorRegistry.get( QueryLanguage.MONGO_QL, OperatorName.MQL_UPDATE_REPLACE ) ),
        REMOVE( OperatorRegistry.get( QueryLanguage.MONGO_QL, OperatorName.MQL_UPDATE_REMOVE ) );

        @Getter
        private final Operator operator;


        UpdateOperation( Operator operator ) {
            this.operator = operator;
        }
    }

}
