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

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map.Entry;
import java.util.Objects;
import org.bson.BsonDocument;
import org.bson.BsonValue;
import org.polypheny.db.mql.Mql;
import org.polypheny.db.mql.MqlFind;
import org.polypheny.db.mql.MqlNode;
import org.polypheny.db.plan.RelOptCluster;
import org.polypheny.db.prepare.PolyphenyDbCatalogReader;
import org.polypheny.db.processing.MqlProcessor;
import org.polypheny.db.rel.RelNode;
import org.polypheny.db.rel.RelRoot;
import org.polypheny.db.rel.logical.LogicalFilter;
import org.polypheny.db.rel.logical.LogicalProject;
import org.polypheny.db.rel.logical.LogicalTableScan;
import org.polypheny.db.rel.type.RelDataType;
import org.polypheny.db.rel.type.RelDataTypeSystem;
import org.polypheny.db.rex.RexCall;
import org.polypheny.db.rex.RexInputRef;
import org.polypheny.db.rex.RexLiteral;
import org.polypheny.db.rex.RexNode;
import org.polypheny.db.sql.SqlKind;
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
        Mql.Type kind = query.getKind();
        switch ( kind ) {
            case FIND:
                return RelRoot.of( convertFind( (MqlFind) query ), SqlKind.SELECT );
            default:
                throw new IllegalStateException( "Unexpected value: " + kind );
        }

    }


    private RelNode convertFind( MqlFind query ) {
        //RelOptTable table = SqlValidatorUtil.getRelOptTable( null, null, null, null );
        RelNode node = LogicalTableScan.create( cluster, catalogReader.getTable( Collections.singletonList( query.getCollection() ) ) );
        if ( query.getQuery() != null && !query.getQuery().isEmpty() ) {
            node = combineFilter( query.getQuery(), node );
        }

        if ( query.getProjection() != null && !query.getProjection().isEmpty() ) {
            node = convertProjection( query.getProjection(), node );
        }
        return node;

    }


    private RelNode combineFilter( BsonDocument filter, RelNode node ) {
        RelNode output = null;
        if ( filter.size() == 1 ) {
            output = convertFilter( filter.getFirstKey(), filter.get( filter.getFirstKey() ), node );
        } else {
            List<RexNode> operands = new ArrayList<>();
            for ( Entry<String, BsonValue> entry : filter.entrySet() ) {
                //operands.add( new RexLiteral( getComparable( entry.getValue() ),  ) )
            }
        }
        return output;
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


    private RelNode convertFilter( String firstKey, BsonValue bsonValue, RelNode node ) {
        RelDataType type = new PolyTypeFactoryImpl( RelDataTypeSystem.DEFAULT ).createPolyType( PolyType.BOOLEAN );
        RelDataType keyType = new PolyTypeFactoryImpl( RelDataTypeSystem.DEFAULT ).createPolyType( PolyType.VARCHAR, 20 );
        RelDataType valueType = new PolyTypeFactoryImpl( RelDataTypeSystem.DEFAULT ).createPolyType( getPolyType( bsonValue ) );
        List<RexNode> operands = new ArrayList<>();
        Pair<Comparable, PolyType> valuePair = RexLiteral.convertType( getComparable( bsonValue, valueType ), valueType );

        operands.add( new RexInputRef( 3, keyType ) );
        operands.add( new RexLiteral( valuePair.left, valueType, valuePair.right ) );
        RexCall condition = new RexCall( type, SqlStdOperatorTable.EQUALS, operands );
        return LogicalFilter.create( node, condition );
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


    private RelNode convertProjection( BsonDocument projection, RelNode tableScan ) {
        return LogicalProject.create( tableScan, Collections.emptyList(), (List<String>) null );
    }

}
