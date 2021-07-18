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
import org.bson.BsonBoolean;
import org.bson.BsonDocument;
import org.bson.BsonInt32;
import org.bson.BsonRegularExpression;
import org.bson.BsonString;
import org.bson.BsonValue;
import org.polypheny.db.config.RuntimeConfig;
import org.polypheny.db.document.DocumentTypeUtil;
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
import org.polypheny.db.rel.type.RelDataType;
import org.polypheny.db.rel.type.RelDataTypeFactory;
import org.polypheny.db.rel.type.RelDataTypeField;
import org.polypheny.db.rex.RexCall;
import org.polypheny.db.rex.RexInputRef;
import org.polypheny.db.rex.RexLiteral;
import org.polypheny.db.rex.RexNode;
import org.polypheny.db.sql.SqlKind;
import org.polypheny.db.sql.SqlOperator;
import org.polypheny.db.sql.fun.SqlStdOperatorTable;
import org.polypheny.db.type.PolyType;
import org.polypheny.db.util.Pair;

public class MqlToRelConverter {

    private final PolyphenyDbCatalogReader catalogReader;
    private final RelOptCluster cluster;
    private final static Map<String, SqlOperator> mappings;
    private final static List<String> operators;
    private final static Map<String, List<SqlOperator>> gates;
    private final static Map<String, SqlOperator> mathOperators;
    private final RelDataType any;
    private final RelDataType nullableAny;


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

        // special cases
        operators.add( "$literal" );
        operators.add( "$type" );
        operators.add( "$expr" );
        operators.add( "$jsonSchema" );
        operators.add( "$all" );
    }


    private boolean inQuery = false;


    public MqlToRelConverter( MqlProcessor mqlProcessor, PolyphenyDbCatalogReader catalogReader, RelOptCluster cluster ) {
        this.catalogReader = catalogReader;
        this.cluster = Objects.requireNonNull( cluster );
        this.any = this.cluster.getTypeFactory().createPolyType( PolyType.ANY );
        this.nullableAny = this.cluster.getTypeFactory().createTypeWithNullability( any, true );
    }


    public RelRoot convert( MqlNode query, boolean b, boolean b1 ) {
        RelOptTable table;
        RelNode node;
        Mql.Type kind = query.getKind();

        switch ( kind ) {
            case FIND:
                table = catalogReader.getTable( ImmutableList.of( "private", ((MqlFind) query).getCollection() ) );
                node = LogicalTableScan.create( cluster, table );
                RelNode find = convertFind( (MqlFind) query, table.getRowType(), node );
                return RelRoot.of( find, find.getRowType(), SqlKind.SELECT );
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
            this.inQuery = true;
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
            field = getDefaultDataField( rowType );
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
                        id = getIdentifier( parentKey, rowType, field );
                    }
                    RexNode node = convertMath( key, null, bsonValue, rowType, field, false );

                    if ( losesContext ) {
                        RelDataType type = cluster.getTypeFactory().createPolyType( PolyType.BOOLEAN );
                        return new RexCall( type, SqlStdOperatorTable.DOC_EQ, Arrays.asList( id, node ) );
                    } else {
                        return node;
                    }

                } else {
                    if ( key.equals( "$exists" ) ) {
                        return convertExists( bsonValue, parentKey, rowType, field );
                    } else if ( key.equals( "$type" ) ) {
                        return convertType( bsonValue, parentKey, rowType, field );
                    } else if ( key.equals( "$expr" ) ) {
                        return convertExpr( bsonValue, parentKey, rowType, field );
                    } else if ( key.equals( "$jsonSchema" ) ) {
                        // jsonSchema is a general match
                        return convertJsonSchema( bsonValue, rowType, field );
                    } else if ( key.equals( "$all" ) ) {
                        return convertAll( bsonValue, parentKey, rowType, field );
                    }

                    return translateLogical( key, parentKey, bsonValue, rowType, field );
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


    private RexNode convertMath( String key, String parentKey, BsonValue bsonValue, RelDataType rowType, RelDataTypeField field, boolean isExpr ) {
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
            List<RexNode> nodes = convertArray( parentKey, bsonValue.asArray(), true, rowType, field, errorMsg );

            return getFixedCall( nodes, op, isExpr ? PolyType.BOOLEAN : PolyType.ANY );
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
            return getFixedCall( operands, op, PolyType.BOOLEAN );
        } else {
            throw new RuntimeException( errorMsg );
        }
    }


    private List<RexNode> convertArray( String parentKey, BsonArray bsonValue, boolean allowsLiteral, RelDataType rowType, RelDataTypeField field, String errorMsg ) {
        List<RexNode> operands = new ArrayList<>();
        for ( BsonValue value : bsonValue ) {
            if ( value.isDocument() ) {
                operands.add( translateDocument( value.asDocument(), rowType, field, parentKey ) );
            } else if ( value.isString() && value.asString().getValue().startsWith( "$" ) ) {
                operands.add( getIdentifier( value.asString().getValue().substring( 1 ), rowType, field ) );
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
            nodes.add( getIdentifier( parentKey, rowType, field ) );

            if ( bsonValue.isArray() ) {
                List<RexNode> arr = convertArray( parentKey, bsonValue.asArray(), true, rowType, field, "" );
                nodes.add( getArray( arr,
                        cluster.getTypeFactory().createArrayType( nullableAny, arr.size() ) ) );
            } else {
                nodes.add( convertLiteral( bsonValue ) );
            }

            return getFixedCall( nodes, SqlStdOperatorTable.DOC_EQ, PolyType.BOOLEAN );
        }

    }


    private RexNode getIdentifier( String parentKey, RelDataType rowType, RelDataTypeField field ) {
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


    private RexNode translateDocument( BsonDocument bsonDocument, RelDataType rowType, RelDataTypeField field, String parentKey ) {
        ArrayList<RexNode> operands = new ArrayList<>();

        if ( bsonDocument.getFirstKey().equals( "$regex" ) ) {
            operands.add( convertRegex( bsonDocument, parentKey, field, rowType ) );
        }

        for ( Entry<String, BsonValue> entry : bsonDocument.entrySet() ) {
            if ( entry.getKey().equals( "$regex" ) ) {
                operands.add( convertRegex( bsonDocument, parentKey, field, rowType ) );
            } else if ( !entry.getKey().equals( "$options" ) ) {
                // normal handling
                operands.add( convertEntry( entry.getKey(), parentKey, entry.getValue(), rowType, field ) );
            }
        }
        return getFixedCall( operands, SqlStdOperatorTable.AND, PolyType.BOOLEAN );
    }


    private RexNode translateLogical( String key, String parentKey, BsonValue bsonValue, RelDataType rowType, RelDataTypeField field ) {
        SqlOperator op;
        List<RexNode> nodes = new ArrayList<>();
        op = mappings.get( key );
        switch ( op.kind ) {
            case IN:
            case NOT_IN:
                return convertIn( bsonValue, op, parentKey, rowType, field );
            default:
                if ( parentKey != null ) {
                    nodes.add( getIdentifier( parentKey, rowType, field ) );
                }
                nodes.add( convertLiteral( bsonValue ) );
                return new RexCall( cluster.getTypeFactory().createPolyType( PolyType.BOOLEAN ), op, nodes );
        }

    }


    private RexNode convertRegex( BsonDocument bsonDocument, String parentKey, RelDataTypeField field, RelDataType rowType ) {
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

        return getRegex( stringRegex, options, parentKey, field, rowType );
    }


    private RexCall getRegex( String stringRegex, String options, String parentKey, RelDataTypeField field, RelDataType rowType ) {
        return new RexCall(
                cluster.getTypeFactory().createPolyType( PolyType.BOOLEAN ),
                SqlStdOperatorTable.DOC_REGEX_MATCH,
                Arrays.asList(
                        getIdentifier( parentKey, rowType, field ),
                        convertLiteral( new BsonString( stringRegex ) ),
                        convertLiteral( new BsonBoolean( options.contains( "i" ) ) ),
                        convertLiteral( new BsonBoolean( options.contains( "m" ) ) ),
                        convertLiteral( new BsonBoolean( options.contains( "x" ) ) ),
                        convertLiteral( new BsonBoolean( options.contains( "s" ) ) )
                ) );
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


    private RexNode convertExpr( BsonValue bsonValue, String parentKey, RelDataType rowType, RelDataTypeField field ) {
        if ( bsonValue.isDocument() && bsonValue.asDocument().size() == 1 ) {
            BsonDocument doc = bsonValue.asDocument();
            return convertMath( doc.getFirstKey(), parentKey, doc.get( doc.getFirstKey() ), rowType, field, true );

        } else {
            throw new RuntimeException( "After $expr there needs to be a document with a single entry" );
        }
    }


    private RexNode convertJsonSchema( BsonValue bsonValue, RelDataType rowType, RelDataTypeField field ) {
        if ( bsonValue.isDocument() ) {
            return new RexCall( nullableAny, SqlStdOperatorTable.DOC_JSON_MATCH, Collections.singletonList( RexInputRef.of( field.getIndex(), rowType ) ) );
        } else {
            throw new RuntimeException( "After $jsonSchema there needs to follow a document" );
        }
    }


    private RexNode convertAll( BsonValue bsonValue, String parentKey, RelDataType rowType, RelDataTypeField field ) {
        RelDataType type = cluster.getTypeFactory().createTypeWithNullability( cluster.getTypeFactory().createPolyType( PolyType.BOOLEAN ), true );
        if ( bsonValue.isArray() ) {
            List<RexNode> arr = convertArray( parentKey, bsonValue.asArray(), true, rowType, field, "" );
            RexNode id = getIdentifier( parentKey, rowType, field );

            List<RexNode> operands = new ArrayList<>();
            for ( RexNode rexNode : arr ) {
                operands.add( new RexCall( type, SqlStdOperatorTable.DOC_EQ, Arrays.asList( id, rexNode ) ) );
            }

            return getFixedCall( operands, SqlStdOperatorTable.AND, PolyType.BOOLEAN );
        } else {
            throw new RuntimeException( "After $all there needs to follow a array" );
        }

    }


    private RexNode convertType( BsonValue value, String parentKey, RelDataType rowType, RelDataTypeField field ) {
        String errorMsg = "$type needs either a array of type names or numbers or a single number";
        RexCall types;
        if ( value.isArray() ) {
            List<Integer> numbers = new ArrayList<>();
            for ( BsonValue bsonValue : value.asArray() ) {
                if ( bsonValue.isString() || bsonValue.isInt32() ) {
                    numbers.add( bsonValue.isInt32() ? bsonValue.asInt32().getValue() : DocumentTypeUtil.getTypeNumber( bsonValue.asString().getValue() ) );
                } else {
                    throw new RuntimeException( errorMsg );
                }
            }
            types = getIntArray( numbers );
        } else if ( value.isInt32() || value.isString() ) {
            int typeNumber = value.isInt32() ? value.asInt32().getValue() : DocumentTypeUtil.getTypeNumber( value.asString().getValue() );
            types = getIntArray( Collections.singletonList( typeNumber ) );
        } else {
            throw new RuntimeException( errorMsg );
        }
        return new RexCall(
                cluster.getTypeFactory().createPolyType( PolyType.BOOLEAN ),
                SqlStdOperatorTable.DOC_TYPE_MATCH,
                Arrays.asList( getIdentifier( parentKey, rowType, field ), types ) );

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


    private RexNode translateJsonValue( int index, RelDataType rowType, String key ) {

        RexCall filter = getStringArray( Arrays.asList( key.split( "\\." ) ) );
        return new RexCall( any, SqlStdOperatorTable.DOC_QUERY_VALUE, Arrays.asList( RexInputRef.of( index, rowType ), filter ) );

    }


    private RexNode translateJsonQuery( int index, RelDataType rowType, String key, List<String> excludes ) {
        RelDataTypeFactory factory = cluster.getTypeFactory();
        RelDataType type = factory.createPolyType( PolyType.ANY );
        RelDataType anyType = factory.createTypeWithNullability( type, true );

        RexCall filter = getNestedArray( excludes.stream().map( e -> Arrays.asList( e.split( "\\." ) ) ).collect( Collectors.toList() ) );
        return new RexCall( anyType, SqlStdOperatorTable.DOC_QUERY_EXCLUDE, Arrays.asList( RexInputRef.of( index, rowType ), filter ) );

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


    private RexNode convertIn( BsonValue bsonValue, SqlOperator op, String key, RelDataType rowType, RelDataTypeField field ) {
        RelDataType type = cluster.getTypeFactory().createTypeWithNullability( cluster.getTypeFactory().createPolyType( PolyType.BOOLEAN ), true );

        List<RexNode> operands = new ArrayList<>();
        boolean isIn = op == SqlStdOperatorTable.IN;
        op = isIn ? SqlStdOperatorTable.OR : SqlStdOperatorTable.AND;
        RexNode id = getIdentifier( key, rowType, field );

        for ( BsonValue literal : bsonValue.asArray() ) {
            if ( literal.isDocument() ) {
                throw new RuntimeException( "Non-literal in $in clauses are not supported" );
            }
            if ( literal.isRegularExpression() ) {
                RexNode filter = getRegex( literal.asRegularExpression().getPattern(), literal.asRegularExpression().getOptions(), key, field, rowType );
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


    private RelNode combineProjection( BsonDocument projection, RelNode node, RelDataType rowType ) {
        Map<String, RexNode> includes = new HashMap<>();
        List<String> excludes = new ArrayList<>();

        for ( Entry<String, BsonValue> entry : projection.entrySet() ) {
            BsonValue value = entry.getValue();
            if ( value.isInt32() ) {
                // we have a simple projection; [name]: 1 (include) or [name]:0 (exclude)
                RelDataTypeField field = getTypeFieldOrDefault( rowType, entry.getKey() );

                if ( value.asInt32().getValue() == 1 ) {
                    includes.put( entry.getKey(), getIdentifier( entry.getKey(), rowType, field ) );
                } else if ( value.asInt32().getValue() == 0 ) {
                    excludes.add( entry.getKey() );
                }

            } else if ( value.isString() && value.asString().getValue().startsWith( "$" ) ) {
                // we have a renaming; [new name]: $[old name] ( this counts as a inclusion projection
                String oldName = value.asString().getValue().substring( 1 );
                RelDataTypeField field = getTypeFieldOrDefault( rowType, oldName );

                includes.put( entry.getKey(), getIdentifier( oldName, rowType, field ) );
            } else if ( value.isDocument() && value.asDocument().size() == 1 && value.asDocument().getFirstKey().startsWith( "$" ) ) {
                String func = value.asDocument().getFirstKey();
                RelDataTypeField field = getDefaultDataField( rowType );
                includes.put( entry.getKey(), convertMath( func, entry.getKey(), value.asDocument().get( func ), rowType, field, false ) );
            } else {
                throw new RuntimeException( "After a projection there needs to be either a number, a renaming, a literal or a function." );
            }

        }

        if ( includes.size() != 0 && excludes.size() != 0 ) {
            throw new RuntimeException( "Include projection and exclude projections are not possible at the same time." );
        }

        if ( excludes.size() > 0 ) {
            // exclusion projections only work for the underlying _data field
            RelDataTypeField defaultDataField = getDefaultDataField( rowType );

            List<RexNode> values = new ArrayList<>( Collections.singletonList( translateJsonQuery( defaultDataField.getIndex(), rowType, null, excludes ) ) );
            List<String> names = new ArrayList<>( Collections.singletonList( defaultDataField.getName() ) );

            if ( !excludes.contains( "_id" ) ) {
                names.add( 0, "_id" );
                values.add( 0, RexInputRef.of( 0, rowType ) );
            }

            return LogicalProject.create( node, values, names );
        } else if ( includes.size() > 0 ) {
            List<RexNode> values = new ArrayList<>( includes.values() );
            List<String> names = new ArrayList<>( includes.keySet() );

            if ( !includes.containsKey( "_id" ) ) {
                names.add( 0, "_id" );
                values.add( 0, RexInputRef.of( 0, rowType ) );
            }

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
