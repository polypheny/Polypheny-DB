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
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Objects;
import java.util.stream.Collectors;
import org.bson.BsonArray;
import org.bson.BsonDocument;
import org.bson.BsonValue;
import org.polypheny.db.config.RuntimeConfig;
import org.polypheny.db.jdbc.JavaTypeFactoryImpl;
import org.polypheny.db.mql.Mql;
import org.polypheny.db.mql.MqlAggregate;
import org.polypheny.db.mql.MqlFind;
import org.polypheny.db.mql.MqlInsert;
import org.polypheny.db.mql.MqlNode;
import org.polypheny.db.plan.RelOptCluster;
import org.polypheny.db.plan.RelOptTable;
import org.polypheny.db.prepare.PolyphenyDbCatalogReader;
import org.polypheny.db.processing.MqlProcessor;
import org.polypheny.db.rel.RelCollation;
import org.polypheny.db.rel.RelCollations;
import org.polypheny.db.rel.RelNode;
import org.polypheny.db.rel.RelRoot;
import org.polypheny.db.rel.core.TableModify.Operation;
import org.polypheny.db.rel.logical.LogicalDocuments;
import org.polypheny.db.rel.logical.LogicalFilter;
import org.polypheny.db.rel.logical.LogicalProject;
import org.polypheny.db.rel.logical.LogicalSort;
import org.polypheny.db.rel.logical.LogicalTableModify;
import org.polypheny.db.rel.logical.LogicalTableScan;
import org.polypheny.db.rel.type.DynamicRecordTypeImpl;
import org.polypheny.db.rel.type.RelDataType;
import org.polypheny.db.rel.type.RelDataTypeFactory;
import org.polypheny.db.rel.type.RelDataTypeField;
import org.polypheny.db.rex.RexCall;
import org.polypheny.db.rex.RexInputRef;
import org.polypheny.db.rex.RexLiteral;
import org.polypheny.db.rex.RexNode;
import org.polypheny.db.sql.SqlBinaryOperator;
import org.polypheny.db.sql.SqlCollation;
import org.polypheny.db.sql.SqlJsonValueEmptyOrErrorBehavior;
import org.polypheny.db.sql.SqlKind;
import org.polypheny.db.sql.SqlOperator;
import org.polypheny.db.sql.fun.SqlStdOperatorTable;
import org.polypheny.db.type.PolyType;
import org.polypheny.db.util.NlsString;
import org.polypheny.db.util.Pair;

public class MqlToRelConverter {

    private final PolyphenyDbCatalogReader catalogReader;
    private final RelOptCluster cluster;
    private final static Map<String, SqlOperator> mappings;
    private final static List<String> operators;
    private final static Map<String, List<SqlOperator>> gates;
    private final static Map<String, SqlOperator> mathOperators;


    static {
        gates = new HashMap<>();
        gates.put( "$and", Collections.singletonList( SqlStdOperatorTable.AND ) );
        gates.put( "$or", Collections.singletonList( SqlStdOperatorTable.OR ) );
        gates.put( "$nor", Arrays.asList( SqlStdOperatorTable.AND, SqlStdOperatorTable.NOT ) );
        gates.put( "$not", Collections.singletonList( SqlStdOperatorTable.NOT ) );

        mappings = new HashMap<>();

        mappings.put( "$lt", SqlStdOperatorTable.LESS_THAN );
        mappings.put( "$gt", SqlStdOperatorTable.GREATER_THAN );
        mappings.put( "$eq", SqlStdOperatorTable.EQUALS );
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

        operators = new ArrayList<>();
        operators.addAll( mappings.keySet() );
        operators.addAll( gates.keySet() );
        operators.addAll( mathOperators.keySet() );
    }


    public MqlToRelConverter( MqlProcessor mqlProcessor, PolyphenyDbCatalogReader catalogReader, RelOptCluster cluster ) {
        this.catalogReader = catalogReader;
        this.cluster = Objects.requireNonNull( cluster );

    }


    public RelRoot convert( MqlNode query, boolean b, boolean b1 ) {
        RelOptTable table;
        RelNode node;
        Mql.Type kind = query.getKind();

        switch ( kind ) {
            case FIND:
                table = catalogReader.getTable( ImmutableList.of( "private", ((MqlFind) query).getCollection() ) );
                node = LogicalTableScan.create( cluster, table );
                return RelRoot.of( convertFind( (MqlFind) query, table.getRowType(), node ), SqlKind.SELECT );
            case AGGREGATE:
                table = catalogReader.getTable( Collections.singletonList( ((MqlAggregate) query).getCollection() ) );
                node = LogicalTableScan.create( cluster, table );
                return RelRoot.of( convertAggregate( (MqlAggregate) query, table.getRowType(), node ), SqlKind.SELECT );
            case INSERT:
                table = catalogReader.getTable( ImmutableList.of( "private", ((MqlInsert) query).getCollection() ) );

                return RelRoot.of(
                        LogicalTableModify.create(
                                table,
                                catalogReader,
                                convertMultipleValues( ((MqlInsert) query).getArray() ),
                                Operation.INSERT,
                                null,
                                null,
                                false ),
                        SqlKind.INSERT );
            default:
                throw new IllegalStateException( "Unexpected value: " + kind );
        }

    }


    private RelNode convertMultipleValues( BsonArray array ) {
        List<ImmutableList<Object>> values = new ArrayList<>();
        List<RelDataType> rowTypes = new ArrayList<>();
        for ( BsonValue value : array ) {
            RelDataType rowType = new DynamicRecordTypeImpl( new JavaTypeFactoryImpl() );
            values.add( convertValues( value.asDocument(), rowType ) );
            rowTypes.add( rowType );
        }

        return LogicalDocuments.create( cluster, rowTypes, ImmutableList.copyOf( values ) );
    }


    private ImmutableList<Object> convertValues( BsonDocument doc, RelDataType rowType ) {
        List<Object> values = new ArrayList<>();

        for ( Entry<String, BsonValue> entry : doc.entrySet() ) {
            rowType.getField( entry.getKey(), false, false );
            values.add( convertSingleEntry( entry ) );
        }

        return ImmutableList.copyOf( values );
    }


    private Object convertSingleEntry( Entry<String, BsonValue> entry ) {
        if ( entry.getValue().isDocument() ) {
            Map<String, Object> entries = new HashMap<>();
            for ( Entry<String, BsonValue> docEntry : entry.getValue().asDocument().entrySet() ) {
                entries.put( docEntry.getKey(), convertSingleEntry( docEntry ) );
            }
            return entries;
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

        for ( BsonValue value : query.getPipeline() ) {
            if ( !value.isDocument() && ((BsonDocument) value).size() > 1 ) {
                throw new RuntimeException( "The aggregation pipeline is not used correctly." );
            }
            switch ( ((BsonDocument) value).getFirstKey() ) {
                case "$match":
                    node = combineFilter( value.asDocument().getDocument( "$match" ), node, rowType );
                    break;
                case "$project":
                    node = combineProjection( value.asDocument().getDocument( "$project" ), node, rowType ); // todo dl change rowtype when renames happened

                    // update the rowType due to potential renamings
                    if ( rowType != null ) {
                        rowType = node.getRowType();
                    }

                    break;
                // todo dl add more pipeline statements
                default:
                    throw new IllegalStateException( "Unexpected value: " + ((BsonDocument) value).getFirstKey() );
            }
        }

        return node;
    }


    private RelNode convertFind( MqlFind query, RelDataType rowType, RelNode node ) {
        if ( query.getQuery() != null && !query.getQuery().isEmpty() ) {
            node = combineFilter( query.getQuery(), node, rowType );
        }

        if ( query.getProjection() != null && !query.getProjection().isEmpty() ) {
            node = combineProjection( query.getProjection(), node, rowType );
        }

        return node;

    }


    private RelNode wrapLimit( RelNode node, int limit ) {
        final RelCollation collation = cluster.traitSet().canonize( RelCollations.of( new ArrayList<>() ) );
        return LogicalSort.create(
                node,
                collation,
                new RexLiteral(
                        new BigDecimal( 0 ),
                        cluster.getTypeFactory()
                                .createPolyType( PolyType.INTEGER ), PolyType.DECIMAL ),
                new RexLiteral(
                        new BigDecimal( limit ),
                        cluster.getTypeFactory()
                                .createPolyType( PolyType.INTEGER ), PolyType.DECIMAL )
        );
    }


    private RelNode combineFilter( BsonDocument filter, RelNode node, RelDataType rowType ) {
        RexNode condition = translateDocument( filter, rowType, null, null );

        return LogicalFilter.create( node, condition );
    }


    private Comparable<?> getComparable( BsonValue value ) {
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


    private RexNode convertEntry( String key, String parentKey, BsonValue bsonValue, RelDataType rowType, RelDataTypeField field ) {
        if ( field == null ) {
            field = rowType.getField( "_data", false, false );
        }

        List<RexNode> operands = new ArrayList<>();
        if ( !key.startsWith( "$" ) ) {
            return convertField( parentKey == null ? key : parentKey + "." + key, bsonValue, rowType, field );
        } else {

            if ( operators.contains( key ) ) {
                if ( gates.containsKey( key ) ) {
                    return convertGate( key, parentKey, bsonValue, rowType, field );
                } else if ( mathOperators.containsKey( key ) ) {

                    boolean losesContext = parentKey != null;
                    RexNode id = null;
                    if ( losesContext ) {
                        // we lose context to the parent and have to "drop it" as we move into $subtract or $eq
                        id = attachParentIdentifier( parentKey, rowType, field );
                    }
                    RexNode node = convertMath( key, null, bsonValue, rowType, field );

                    if ( losesContext ) {
                        RelDataType type = cluster.getTypeFactory().createPolyType( PolyType.BOOLEAN );
                        return new RexCall( type, SqlStdOperatorTable.EQUALS, Arrays.asList( id, node ) );
                    } else {
                        return node;
                    }

                } else {
                    return translateLogical( key, parentKey, bsonValue, rowType, field );
                }
            } else {
                // handle others
            }
        }

        return getFixedCall( operands, SqlStdOperatorTable.AND );
    }


    private RexNode convertMath( String key, String parentKey, BsonValue bsonValue, RelDataType rowType, RelDataTypeField field ) {
        SqlOperator op = mathOperators.get( key );
        String errorMsg = "After a " + String.join( ",", mathOperators.keySet() ) + " a list of literal or documents is needed.";
        if ( bsonValue.isArray() ) {
            List<RexNode> nodes = convertArray( parentKey, bsonValue.asArray(), true, rowType, field, errorMsg );

            return getFixedCall( nodes, op );
        } else {
            throw new RuntimeException( errorMsg );
        }
    }


    private RexNode convertGate( String key, String parentKey, BsonValue bsonValue, RelDataType rowType, RelDataTypeField field ) {

        SqlOperator op;
        switch ( key ) {
            case "$and":
                op = SqlStdOperatorTable.AND;
                return convertLogicalArray( parentKey, bsonValue, rowType, op, false, field );
            case "$or":
                op = SqlStdOperatorTable.OR;
                return convertLogicalArray( parentKey, bsonValue, rowType, op, false, field );
            case "$nor":
                op = SqlStdOperatorTable.OR;
                return convertLogicalArray( parentKey, bsonValue, rowType, op, true, field );
            case "$not":
                op = SqlStdOperatorTable.NOT;
                if ( bsonValue.isDocument() ) {
                    RelDataType type = cluster.getTypeFactory().createPolyType( PolyType.BOOLEAN );
                    return new RexCall( type, op, Collections.singletonList( translateDocument( bsonValue.asDocument(), rowType, field, parentKey ) ) );
                } else {
                    throw new RuntimeException( "After a $not a document is needed" );
                }

            default:
                throw new RuntimeException( "This logical operator was not recognized:" );
        }
    }


    private RexNode convertLogicalArray( String parentKey, BsonValue bsonValue, RelDataType rowType, SqlOperator op, boolean isNegated, RelDataTypeField field ) {
        String errorMsg = "After logical operators \"$and\",\"$or\" and \"nor\" an array of documents is needed";
        if ( bsonValue.isArray() ) {
            List<RexNode> operands = convertArray( parentKey, bsonValue.asArray(), false, rowType, field, errorMsg );
            if ( isNegated ) {
                operands = operands.stream().map( this::negate ).collect( Collectors.toList() );
            }
            return getFixedCall( operands, op );
        } else {
            throw new RuntimeException( errorMsg );
        }
    }


    private List<RexNode> convertArray( String parentKey, BsonArray bsonValue, boolean allowsLiteral, RelDataType rowType, RelDataTypeField field, String errorMsg ) {
        List<RexNode> operands = new ArrayList<>();
        for ( BsonValue value : bsonValue ) {
            if ( value.isDocument() ) {
                operands.add( translateDocument( value.asDocument(), rowType, field, parentKey ) );
            } else if ( allowsLiteral ) {
                operands.add( convertLiteral( value ) );
            } else {
                throw new RuntimeException( errorMsg );
            }
        }
        return operands;
    }


    private RexNode convertField( String parentKey, BsonValue bsonValue, RelDataType rowType, RelDataTypeField field ) {

        if ( bsonValue.isDocument() ) {
            // we have a document where the sub-keys are either logical like "$eq, $or"
            // we don't attach the id yet as we maybe have sub-documents
            return translateDocument( bsonValue.asDocument(), rowType, field, parentKey );
        } else {
            // we have a simple assignment to a value, can attach and translate the value
            List<RexNode> nodes = new ArrayList<>();
            nodes.add( attachParentIdentifier( parentKey, rowType, field ) );
            nodes.add( convertLiteral( bsonValue ) );
            return new RexCall( cluster.getTypeFactory().createPolyType( PolyType.BOOLEAN ), SqlStdOperatorTable.EQUALS, nodes );
        }

    }


    private RexNode attachParentIdentifier( String parentKey, RelDataType rowType, RelDataTypeField field ) {
        if ( !rowType.getFieldNames().contains( parentKey ) ) {
            return translateJsonValue( field.getIndex(), rowType, parentKey );
        } else {
            return attachRef( parentKey, rowType );
        }
    }


    private RexNode attachRef( String parentKey, RelDataType rowType ) {
        RelDataTypeField field = rowType.getField( parentKey, false, false );
        return RexInputRef.of( field.getIndex(), rowType );
    }


    private RexNode getFixedCall( List<RexNode> operands, SqlOperator op ) {
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
                if ( operand instanceof RexCall && ((RexCall) operand).op.kind == op.kind ) {
                    toAdd.addAll( ((RexCall) operand).operands );
                    toRemove.add( operand );
                }
            }
            if ( toAdd.size() > 0 ) {
                operands.addAll( toAdd );
                operands.removeAll( toRemove );
            }

            return new RexCall( cluster.getTypeFactory().createPolyType( PolyType.BOOLEAN ), op, operands );
        }
    }


    private RexNode translateDocument( BsonDocument bsonDocument, RelDataType rowType, RelDataTypeField field, String parentKey ) {
        ArrayList<RexNode> operands = new ArrayList<>();
        for ( Entry<String, BsonValue> entry : bsonDocument.entrySet() ) {
            operands.add( convertEntry( entry.getKey(), parentKey, entry.getValue(), rowType, field ) );
        }
        return getFixedCall( operands, SqlStdOperatorTable.AND );
    }


    private RexNode translateLogical( String key, String parentKey, BsonValue bsonValue, RelDataType rowType, RelDataTypeField field ) {
        SqlOperator op;
        List<RexNode> nodes = new ArrayList<>();
        op = mappings.get( key );
        switch ( op.kind ) {
            case IN:
            case NOT_IN:
                return convertIn( bsonValue, (SqlBinaryOperator) op, parentKey, rowType, field );
            case EXISTS:
                return convertExists( bsonValue, parentKey, rowType, field );
            default:
                if ( parentKey != null ) {
                    nodes.add( attachParentIdentifier( parentKey, rowType, field ) );
                }
                nodes.add( convertLiteral( bsonValue ) );
                return new RexCall( cluster.getTypeFactory().createPolyType( PolyType.BOOLEAN ), op, nodes );
        }

        //return new RexCall( cluster.getTypeFactory().createPolyType( PolyType.BOOLEAN ), op, nodes );
    }


    private RexNode convertExists( BsonValue value, String parentKey, RelDataType rowType, RelDataTypeField field ) {
        if ( value.isBoolean() ) {

            int index = rowType.getFieldList().indexOf( field );
            RexNode exists = translateJsonExists( index, rowType, parentKey );
            if ( !value.asBoolean().getValue() ) {
                return negate( exists );
            }
            return exists;

        } else {
            throw new RuntimeException( "$exist without a boolean is not supported" );
        }
    }


    private RexNode negate( RexNode exists ) {
        return new RexCall( cluster.getTypeFactory().createPolyType( PolyType.BOOLEAN ), SqlStdOperatorTable.NOT, Collections.singletonList( exists ) );
    }


    private RexNode translateJsonExists( int index, RelDataType rowType, String key ) {
        RelDataTypeFactory factory = cluster.getTypeFactory();
        RelDataType type = factory.createPolyType( PolyType.ANY );
        RelDataType anyType = factory.createTypeWithNullability( type, true );

        RexCall common = getJsonCommonApi( index, rowType, key, factory, anyType );

        return new RexCall(
                factory.createPolyType( PolyType.BOOLEAN ),
                SqlStdOperatorTable.JSON_EXISTS,
                Collections.singletonList( common ) );
    }


    private RexNode translateJsonValue( int index, RelDataType rowType, String key ) {
        RelDataTypeFactory factory = cluster.getTypeFactory();
        RelDataType type = factory.createPolyType( PolyType.ANY );
        RelDataType anyType = factory.createTypeWithNullability( type, true );

        // match part
        RexCall common = getJsonCommonApi( index, rowType, key, factory, anyType );

        RexLiteral flag = new RexLiteral( SqlJsonValueEmptyOrErrorBehavior.NULL, factory.createPolyType( PolyType.SYMBOL ), PolyType.SYMBOL );

        RexCall value = new RexCall(
                anyType,
                SqlStdOperatorTable.JSON_VALUE_ANY,
                Arrays.asList( common, flag, new RexLiteral( null, anyType, PolyType.NULL, true ),
                        flag,
                        new RexLiteral( null, anyType, PolyType.NULL, true ) ) );

        RelDataType returnAny = factory.createTypeWithNullability( factory.createPolyType( PolyType.VARCHAR, 2000 ), true );
        return new RexCall( returnAny, SqlStdOperatorTable.CAST, Collections.singletonList( value ) );

    }


    private RexCall getJsonCommonApi( int index, RelDataType rowType, String key, RelDataTypeFactory factory, RelDataType anyType ) {
        RexCall ref = new RexCall(
                anyType,
                SqlStdOperatorTable.JSON_VALUE_EXPRESSION,
                Collections.singletonList( RexInputRef.of( index, rowType ) ) );
        String jsonFilter = RuntimeConfig.JSON_MODE.getString() + " $." + key;
        NlsString nlsString = new NlsString( jsonFilter, "ISO-8859-1", SqlCollation.IMPLICIT );
        RexLiteral filter = new RexLiteral( nlsString, factory.createPolyType( PolyType.CHAR, jsonFilter.length() ), PolyType.CHAR );
        return new RexCall( anyType, SqlStdOperatorTable.JSON_API_COMMON_SYNTAX, Arrays.asList( ref, filter ) );
    }


    private RexNode convertLiteral( BsonValue bsonValue ) {

        RelDataType type = getRelDataType( bsonValue );
        Pair<Comparable, PolyType> valuePair = RexLiteral.convertType( getComparable( bsonValue ), type );
        return new RexLiteral( valuePair.left, type, valuePair.right );
    }


    private RexNode convertIn( BsonValue bsonValue, SqlBinaryOperator op, String key, RelDataType rowType, RelDataTypeField field ) {
        RelDataType type = cluster.getTypeFactory().createPolyType( PolyType.BOOLEAN );

        List<RexNode> operands = new ArrayList<>();
        boolean isIn = op == SqlStdOperatorTable.IN;
        op = isIn ? SqlStdOperatorTable.OR : SqlStdOperatorTable.AND;
        RexNode id = attachParentIdentifier( key, rowType, field );

        for ( BsonValue literal : bsonValue.asArray() ) {
            if ( literal.isDocument() ) {
                throw new RuntimeException( "Non-literal in $in clauses are not supported" );
            }
            operands.add( new RexCall( type, isIn ? SqlStdOperatorTable.EQUALS : SqlStdOperatorTable.NOT_EQUALS, Arrays.asList( id, convertLiteral( literal ) ) ) );
        }
        if ( operands.size() == 1 ) {
            return operands.get( 0 );
        }

        return new RexCall( type, op, operands );
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


    private RelNode combineProjection( BsonDocument projection, RelNode node, RelDataType rowType ) {
        List<RelDataTypeField> includes = new ArrayList<>();
        List<RelDataTypeField> excludes = new ArrayList<>();
        List<Pair<String, RelDataTypeField>> renamings = new ArrayList<>();
        for ( Entry<String, BsonValue> entry : projection.entrySet() ) {
            // either [name] : 1 means column is included ore [name]: 0 means included
            // cannot be mixed so we have to handle that as well
            if ( entry.getValue().isInt32() ) {
                // included fields
                if ( entry.getValue().asInt32().getValue() == 1 ) {
                    includes.add( rowType.getField( entry.getKey(), false, false ) );
                } else {
                    excludes.add( rowType.getField( entry.getKey(), false, false ) );
                }
            } else { // we can also have renaming with [new name]:$[name]
                assert entry.getValue().isString() && entry.getValue().asString().getValue().startsWith( "$" );
                String fieldName = entry.getValue().asString().getValue().substring( 1 );
                renamings.add( Pair.of( entry.getKey(), rowType.getField( fieldName, false, false ) ) );
            }
        }
        if ( includes.size() != 0 && excludes.size() != 0 ) {
            throw new RuntimeException( "It is not possible to include and exclude different fields at the same time." );
        }
        List<Pair<Integer, String>> indexes = new ArrayList<>();
        // we have defined which fields have to be projected
        if ( includes.size() != 0 ) {
            for ( RelDataTypeField field : includes ) {
                indexes.add( Pair.of( field.getIndex(), field.getName() ) );
            }
        } else if ( excludes.size() != 0 ) {
            // we have to include all fields except the excluded ones
            for ( RelDataTypeField field : rowType.getFieldList() ) {
                if ( !excludes.contains( field ) ) {
                    indexes.add( Pair.of( field.getIndex(), field.getName() ) );
                }
            }
        }

        List<Integer> includesIndexes = indexes.stream().map( field -> field.left ).collect( Collectors.toList() );
        for ( Pair<String, RelDataTypeField> pair : renamings ) {
            if ( Pair.left( indexes ).contains( pair.right.getIndex() ) && !includesIndexes.contains( pair.right.getIndex() ) ) {
                // if we have already included the field we have to remove it,
                // except if it is explicitly included and renamed at the same time
                indexes.remove( Pair.left( indexes ).indexOf( pair.right.getIndex() ) );
            }
            indexes.add( Pair.of( pair.right.getIndex(), pair.left ) );
            // if for some reason we rename the same field multiple times we stop the removal after the first by adding it here
            includesIndexes.add( pair.right.getIndex() );
        }

        List<RexInputRef> inputRefs = indexes.stream()
                .map( i -> RexInputRef.of( i.left, rowType ) )
                .collect( Collectors.toList() );
        return LogicalProject.create( node, inputRefs, Pair.right( indexes ) );
    }

}
