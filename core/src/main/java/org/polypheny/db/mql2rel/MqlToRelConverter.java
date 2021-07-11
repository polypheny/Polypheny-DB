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
import org.bson.BsonString;
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
import org.polypheny.db.rel.type.RelDataTypeFieldImpl;
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
            Pair<Comparable, PolyType> valuePair = RexLiteral.convertType( getComparable( entry.getValue(), type ), type );
            return new RexLiteral( valuePair.left, type, valuePair.right );
        }
    }


    private BsonValue jsonify( BsonValue value ) {
        if ( value.isDocument() ) {
            return new BsonString( value.asDocument().toJson() );
        }
        return value;
    }


    private RelDataType getRelDataType( BsonValue value ) {
        PolyType polyType = getPolyType( value );
        switch ( polyType ) {
            case JSON:
                return cluster.getTypeFactory().createPolyType( PolyType.CHAR, value.asDocument().toJson().length() );
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
        RexNode condition;
        RelDataType type = cluster.getTypeFactory().createPolyType( PolyType.BOOLEAN );
        List<RexNode> operands = new ArrayList<>();
        for ( Entry<String, BsonValue> entry : filter.entrySet() ) {
            operands.add( convertFilter( entry.getKey(), entry.getValue(), rowType ) );
        }

        if ( operands.size() == 1 ) {
            condition = operands.get( 0 );
        } else {
            condition = new RexCall( type, SqlStdOperatorTable.AND, operands );
        }
        return LogicalFilter.create( node, condition );
    }


    private RexNode convertDocument( BsonDocument doc, RelDataType rowType ) {
        ArrayList<RexNode> operands = new ArrayList<>();
        RelDataType type = cluster.getTypeFactory().createPolyType( PolyType.BOOLEAN );
        for ( Entry<String, BsonValue> entry : doc.entrySet() ) {
            operands.add( convertFilter( entry.getKey(), entry.getValue(), rowType ) );
        }

        if ( operands.size() == 1 ) {
            return operands.get( 0 );
        }
        return new RexCall( type, SqlStdOperatorTable.AND, operands );
    }


    private Comparable<?> getComparable( BsonValue value, RelDataType type ) {
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


    private RexNode convertFilter( String firstKey, BsonValue bsonValue, RelDataType rowType ) {
        if ( !firstKey.startsWith( "$" ) ) {
            return convertFieldEntry( firstKey, bsonValue, rowType );
        } else {
            return convertLogicalEntry( firstKey, bsonValue, rowType );
        }
    }


    private RexNode convertLogicalEntry( String key, BsonValue bsonValue, RelDataType rowType ) {
        RelDataType type = cluster.getTypeFactory().createPolyType( PolyType.BOOLEAN );
        SqlOperator op;
        switch ( key ) {
            case "$and":
                op = SqlStdOperatorTable.AND;
                return convertLogicalArray( bsonValue, rowType, op, false );
            case "$or":
                op = SqlStdOperatorTable.OR;
                return convertLogicalArray( bsonValue, rowType, op, false );

            case "$nor":
                op = SqlStdOperatorTable.OR;
                return convertLogicalArray( bsonValue, rowType, op, true );

            case "$not":
                op = SqlStdOperatorTable.NOT;
                return new RexCall( type, op, Collections.singletonList( convertDocument( bsonValue.asDocument(), rowType ) ) );

            default:
                throw new RuntimeException( "This logical operator was not recognized:" );
        }
    }


    private RexNode convertLogicalArray( BsonValue bsonValue, RelDataType rowType, SqlOperator op, boolean isNegated ) {
        RelDataType type = cluster.getTypeFactory().createPolyType( PolyType.BOOLEAN );
        List<RexNode> operands = new ArrayList<>();
        if ( bsonValue.isArray() ) {
            for ( BsonValue value : bsonValue.asArray() ) {
                if ( value.isDocument() && !isNegated ) {
                    operands.add( convertDocument( value.asDocument(), rowType ) );
                } else if ( value.isDocument() && isNegated ) {
                    operands.add( new RexCall( type, SqlStdOperatorTable.NOT, Collections.singletonList( convertDocument( value.asDocument(), rowType ) ) ) );
                } else {
                    throw new RuntimeException( "After logical operators \"$and\",\"$or\" and \"nor\" an array of documents is needed" );
                }
            }
        } else {
            throw new RuntimeException( "After logical operators \"$and\",\"$or\" and \"nor\" an array of documents is needed" );
        }
        if ( operands.size() == 1 ) {
            return operands.get( 0 );
        }
        return new RexCall( type, op, operands );
    }


    private RexNode convertFieldEntry( String parentKey, BsonValue bsonValue, RelDataType rowType ) {
        List<RexNode> operands = new ArrayList<>();

        RelDataTypeField field;
        if ( !rowType.getFieldNames().contains( parentKey ) ) {
            field = rowType.getField( "_data", false, false );
            //operands.add( RexSubInputRef.of( field.getIndex(), rowType, firstKey ) );
            //operands.add( translateJson( field.getIndex(), field.getName(), rowType, firstKey, bsonValue ) );
            if ( bsonValue.isDocument() ) {
                // we have a document where the sub-keys are either logical like "$eq, $or" or we have a sub-key and need to change the value
                return translateDocument( bsonValue.asDocument(), rowType, operands, field, parentKey );
            } else {
                // we have a simple assignment to a value, can attach and translate the value
                operands.add( translateJsonValue( field.getIndex(), rowType, parentKey ) );
                return translateDocumentOrLiteral( bsonValue, rowType, operands, field );
            }
        } else {
            field = rowType.getField( parentKey, false, false );
            operands.add( RexInputRef.of( field.getIndex(), rowType ) );

            return translateDocumentOrLiteral( bsonValue, rowType, operands, field );
        }

    }


    private RexNode translateDocumentOrLiteral( BsonValue bsonValue, RelDataType rowType, List<RexNode> operands, RelDataTypeField field ) {
        if ( bsonValue.isDocument() ) {
            List<RexNode> nodes = new ArrayList<>();
            for ( Entry<String, BsonValue> entry : bsonValue.asDocument().entrySet() ) {
                nodes.add( translateDocument( (BsonDocument) bsonValue, rowType, operands, field, entry.getKey() ) );
            }

            if ( nodes.size() == 1 ) {
                return nodes.get( 0 );
            } else {
                throw new RuntimeException( "todo" );
            }

        } else {
            return convertLiteral( bsonValue, operands, field, SqlStdOperatorTable.EQUALS );
        }
    }


    private RexNode translateDocument( BsonDocument bsonValue, RelDataType rowType, List<RexNode> operands, RelDataTypeField field, String parentKey ) {
        List<RexNode> nodes = new ArrayList<>();
        for ( Entry<String, BsonValue> entry : bsonValue.entrySet() ) {
            if ( entry.getKey().startsWith( "$" ) ) {
                nodes.add( translateLogical( bsonValue, rowType, operands, field, entry.getKey(), parentKey ) );
            } else {
                nodes.add( convertFieldEntry( parentKey + "." + entry.getKey(), entry.getValue(), rowType ) );
            }
        }

        if ( nodes.size() == 1 ) {
            return nodes.get( 0 );
        } else {
            RelDataType type = cluster.getTypeFactory().createPolyType( PolyType.BOOLEAN );
            return new RexCall( type, SqlStdOperatorTable.AND, nodes );
        }
    }


    private RexNode translateLogical( BsonDocument bsonValue, RelDataType rowType, List<RexNode> operands, RelDataTypeField field, String key, String parentKey ) {
        SqlOperator op;
        op = getBinaryOperator( key );
        if ( op == SqlStdOperatorTable.IN || op == SqlStdOperatorTable.NOT_IN ) {
            return convertIn( bsonValue, rowType, (SqlBinaryOperator) op, field, key );
        } else if ( op.kind == SqlKind.EXISTS ) {
            return convertExists( bsonValue.get( key ), parentKey, rowType, field );
        } else {
            return convertLiteral( bsonValue.get( key ), operands, field, op );
        }
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
        RexCall common = new RexCall( anyType, SqlStdOperatorTable.JSON_API_COMMON_SYNTAX, Arrays.asList( ref, filter ) );
        return common;
    }


    private RelDataTypeField getRelDataTypeField( String name, int index, BsonValue bsonValue ) {
        RelDataType type = getRelDataType( bsonValue );

        return new RelDataTypeFieldImpl( name, index, type );
    }


    private RexNode convertLiteral( BsonValue bsonValue, List<RexNode> operands, RelDataTypeField field, SqlOperator op ) {
        RelDataType type = getRelDataType( bsonValue );
        Pair<Comparable, PolyType> valuePair = RexLiteral.convertType( getComparable( bsonValue, field.getType() ), type );
        RexNode value = new RexLiteral( valuePair.left, type, valuePair.right );
        operands.add( value );

        RelDataType eq = cluster.getTypeFactory().createPolyType( PolyType.BOOLEAN );
        return new RexCall( eq, op, operands );
    }


    private RexNode convertIn( BsonDocument bsonValue, RelDataType rowType, SqlBinaryOperator op, RelDataTypeField field, String key ) {
        RelDataType type = cluster.getTypeFactory().createPolyType( PolyType.BOOLEAN );
        RexNode value;
        List<RexNode> operands = new ArrayList<>();
        boolean isIn = op == SqlStdOperatorTable.IN;
        op = isIn ? SqlStdOperatorTable.OR : SqlStdOperatorTable.AND;

        for ( BsonValue literal : bsonValue.get( key ).asArray() ) {
            if ( literal.isDocument() ) {
                throw new RuntimeException( "Non-literal in $in clauses are not supported" );
            }
            Pair<Comparable, PolyType> valuePair = RexLiteral.convertType( getComparable( literal, field.getType() ), field.getType() );
            value = new RexLiteral( valuePair.left, field.getType(), valuePair.right );
            operands.add(
                    new RexCall( type, isIn ? SqlStdOperatorTable.EQUALS : SqlStdOperatorTable.NOT_EQUALS, Arrays.asList(
                            RexInputRef.of( field.getIndex(), rowType ), value ) ) );
        }
        if ( operands.size() == 1 ) {
            return operands.get( 0 );
        }

        return new RexCall( type, op, operands );
    }


    private SqlOperator getBinaryOperator( String key ) {
        assert key.startsWith( "$" );

        switch ( key ) {
            case "$lt":
                return SqlStdOperatorTable.LESS_THAN;
            case "$gt":
                return SqlStdOperatorTable.GREATER_THAN;
            case "$eq":
                return SqlStdOperatorTable.EQUALS;
            case "$lte":
                return SqlStdOperatorTable.LESS_THAN_OR_EQUAL;
            case "$gte":
                return SqlStdOperatorTable.GREATER_THAN_OR_EQUAL;
            case "$in":
                return SqlStdOperatorTable.IN;
            case "$nin":
                return SqlStdOperatorTable.NOT_IN;
            case "$and":
                return SqlStdOperatorTable.AND;
            case "$or":
                return SqlStdOperatorTable.OR;
            case "$exists":
                return SqlStdOperatorTable.EXISTS;
            default:
                throw new IllegalStateException( "Unexpected value for SqlBinaryOperator: " + key );
        }
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
