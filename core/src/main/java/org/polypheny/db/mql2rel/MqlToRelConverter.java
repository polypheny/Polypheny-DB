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

import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map.Entry;
import java.util.Objects;
import java.util.stream.Collectors;
import org.bson.BsonDocument;
import org.bson.BsonValue;
import org.polypheny.db.mql.Mql;
import org.polypheny.db.mql.MqlAggregate;
import org.polypheny.db.mql.MqlFind;
import org.polypheny.db.mql.MqlNode;
import org.polypheny.db.plan.RelOptCluster;
import org.polypheny.db.plan.RelOptTable;
import org.polypheny.db.prepare.PolyphenyDbCatalogReader;
import org.polypheny.db.processing.MqlProcessor;
import org.polypheny.db.rel.RelCollation;
import org.polypheny.db.rel.RelCollations;
import org.polypheny.db.rel.RelNode;
import org.polypheny.db.rel.RelRoot;
import org.polypheny.db.rel.logical.LogicalFilter;
import org.polypheny.db.rel.logical.LogicalProject;
import org.polypheny.db.rel.logical.LogicalSort;
import org.polypheny.db.rel.logical.LogicalTableScan;
import org.polypheny.db.rel.type.RelDataType;
import org.polypheny.db.rel.type.RelDataTypeField;
import org.polypheny.db.rel.type.RelDataTypeSystem;
import org.polypheny.db.rex.RexCall;
import org.polypheny.db.rex.RexInputRef;
import org.polypheny.db.rex.RexLiteral;
import org.polypheny.db.rex.RexNode;
import org.polypheny.db.sql.SqlBinaryOperator;
import org.polypheny.db.sql.SqlKind;
import org.polypheny.db.sql.SqlOperator;
import org.polypheny.db.sql.fun.SqlStdOperatorTable;
import org.polypheny.db.type.PolyType;
import org.polypheny.db.type.PolyTypeFactoryImpl;
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
                table = catalogReader.getTable( Collections.singletonList( ((MqlFind) query).getCollection() ) );
                node = LogicalTableScan.create( cluster, table );
                return RelRoot.of( convertFind( (MqlFind) query, table.getRowType(), node ), SqlKind.SELECT );
            case AGGREGATE:
                table = catalogReader.getTable( Collections.singletonList( ((MqlAggregate) query).getCollection() ) );
                node = LogicalTableScan.create( cluster, table );
                return RelRoot.of( convertAggregate( (MqlAggregate) query, table.getRowType(), node ), SqlKind.SELECT );
            default:
                throw new IllegalStateException( "Unexpected value: " + kind );
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
                        new PolyTypeFactoryImpl( RelDataTypeSystem.DEFAULT )
                                .createPolyType( PolyType.INTEGER ), PolyType.DECIMAL ),
                new RexLiteral(
                        new BigDecimal( limit ),
                        new PolyTypeFactoryImpl( RelDataTypeSystem.DEFAULT )
                                .createPolyType( PolyType.INTEGER ), PolyType.DECIMAL )
        );
    }


    private RelNode combineFilter( BsonDocument filter, RelNode node, RelDataType rowType ) {
        RexNode condition;
        RelDataType type = new PolyTypeFactoryImpl( RelDataTypeSystem.DEFAULT ).createPolyType( PolyType.BOOLEAN );
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
        RelDataType type = new PolyTypeFactoryImpl( RelDataTypeSystem.DEFAULT ).createPolyType( PolyType.BOOLEAN );
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
        RelDataType type = new PolyTypeFactoryImpl( RelDataTypeSystem.DEFAULT ).createPolyType( PolyType.BOOLEAN );
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
        RelDataType type = new PolyTypeFactoryImpl( RelDataTypeSystem.DEFAULT ).createPolyType( PolyType.BOOLEAN );
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


    private RexNode convertFieldEntry( String firstKey, BsonValue bsonValue, RelDataType rowType ) {
        RelDataType type = new PolyTypeFactoryImpl( RelDataTypeSystem.DEFAULT ).createPolyType( PolyType.BOOLEAN );
        List<RexNode> operands = new ArrayList<>();
        RelDataTypeField field = rowType.getField( firstKey, false, false );
        SqlBinaryOperator op = SqlStdOperatorTable.EQUALS;

        operands.add( RexInputRef.of( field.getIndex(), rowType ) );
        if ( bsonValue.isDocument() ) {
            String key = ((BsonDocument) bsonValue).getFirstKey();
            op = getBinaryOperator( key );
            if ( op == SqlStdOperatorTable.IN || op == SqlStdOperatorTable.NOT_IN ) {
                return convertIn( (BsonDocument) bsonValue, rowType, type, op, field, key );
            } else {
                return convertLiteral( ((BsonDocument) bsonValue).get( key ), operands, field, op );
            }
        } else {
            return convertLiteral( bsonValue, operands, field, op );
        }
    }


    private RexNode convertLiteral( BsonValue bsonValue, List<RexNode> operands, RelDataTypeField field, SqlOperator op ) {
        RexNode value;
        Pair<Comparable, PolyType> valuePair = RexLiteral.convertType( getComparable( bsonValue, field.getType() ), field.getType() );
        value = new RexLiteral( valuePair.left, field.getType(), valuePair.right );
        operands.add( value );

        RelDataType type = new PolyTypeFactoryImpl( RelDataTypeSystem.DEFAULT ).createPolyType( PolyType.BOOLEAN );
        return new RexCall( type, op, operands );
    }


    private RexNode convertIn( BsonDocument bsonValue, RelDataType rowType, RelDataType type, SqlBinaryOperator op, RelDataTypeField field, String key ) {
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


    private SqlBinaryOperator getBinaryOperator( String key ) {
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
                break;
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
