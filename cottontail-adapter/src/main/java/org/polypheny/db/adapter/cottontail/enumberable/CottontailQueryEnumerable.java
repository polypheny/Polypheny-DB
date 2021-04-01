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

package org.polypheny.db.adapter.cottontail.enumberable;


import java.lang.reflect.Method;
import java.math.BigDecimal;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import org.apache.calcite.avatica.util.ByteString;
import org.apache.calcite.linq4j.AbstractEnumerable;
import org.apache.calcite.linq4j.Enumerator;
import org.apache.calcite.linq4j.function.Function1;
import org.apache.calcite.linq4j.tree.Types;
import org.polypheny.db.adapter.DataContext;
import org.polypheny.db.adapter.cottontail.CottontailToEnumerableConverter;
import org.polypheny.db.adapter.cottontail.CottontailWrapper;
import org.polypheny.db.adapter.cottontail.util.CottontailTypeUtil;
import org.polypheny.db.rel.type.RelDataType;
import org.polypheny.db.rel.type.RelDataTypeField;
import org.polypheny.db.sql.fun.SqlArrayValueConstructor;
import org.polypheny.db.type.ArrayType;
import org.vitrivr.cottontail.grpc.CottontailGrpc;
import org.vitrivr.cottontail.grpc.CottontailGrpc.BatchedQueryMessage;
import org.vitrivr.cottontail.grpc.CottontailGrpc.ColumnName;
import org.vitrivr.cottontail.grpc.CottontailGrpc.Knn;
import org.vitrivr.cottontail.grpc.CottontailGrpc.Literal;
import org.vitrivr.cottontail.grpc.CottontailGrpc.Projection;
import org.vitrivr.cottontail.grpc.CottontailGrpc.Projection.ProjectionElement;
import org.vitrivr.cottontail.grpc.CottontailGrpc.Query;
import org.vitrivr.cottontail.grpc.CottontailGrpc.QueryMessage;
import org.vitrivr.cottontail.grpc.CottontailGrpc.QueryResponseMessage;
import org.vitrivr.cottontail.grpc.CottontailGrpc.QueryResponseMessage.Tuple;
import org.vitrivr.cottontail.grpc.CottontailGrpc.Where;


public class CottontailQueryEnumerable<T> extends AbstractEnumerable<T> {

    public static final Method CREATE_QUERY_METHOD = Types.lookupMethod(
            CottontailQueryEnumerable.class,
            "query",
            String.class, String.class, Map.class, Function1.class, Function1.class, Function1.class, Function1.class, DataContext.class, Function1.class, CottontailWrapper.class );

    private final Iterator<QueryResponseMessage> queryIterator;
    private final Function1<Map<String, Literal>, T> rowParser;


    public CottontailQueryEnumerable( Iterator<QueryResponseMessage> queryIterator, Function1<Map<String, Literal>, T> rowParser ) {
        this.queryIterator = queryIterator;
        this.rowParser = rowParser;
    }


    public static CottontailQueryEnumerable<Object> query(
            String from,
            String schema,
            Map<String, String> projection,
            Function1<Map<Long, Object>, Where> whereBuilder,
            Function1<Map<Long, Object>, Knn> knnBuilder, // TODO js(ct) FIGURE OUT
            Function1<Map<Long, Object>, Integer> limitBuilder,
            Function1<Map<Long, Object>, Integer> offsetBuilder,
            DataContext dataContext,
            Function1 rowParser,
            CottontailWrapper wrapper
    ) {
        Iterator<QueryResponseMessage> queryResponseIterator;

        if ( dataContext.getParameterValues().size() < 2 ) {
            final Map<Long, Object> parameterValues;
            if ( dataContext.getParameterValues().size() == 0 ) {
                parameterValues = new HashMap<>();
            } else {
                parameterValues = dataContext.getParameterValues().get( 0 );
            }

            Integer limit = null;
            if ( limitBuilder != null ) {
                limit = limitBuilder.apply( parameterValues );
            }

            Integer offset = null;
            if ( offsetBuilder != null ) {
                offset = offsetBuilder.apply( parameterValues );
            }

            final Query query = buildSingleQuery( from, schema, projection, whereBuilder, knnBuilder, limit, offset, parameterValues );

            /*Query.Builder queryBuilder = Query.newBuilder();

            queryBuilder.setFrom(
                    From.newBuilder().setEntity( Entity.newBuilder().setName( from ).setSchema(
                            Schema.newBuilder().setName( schema ) ) ) );

            if ( limit != null ) {
                queryBuilder.setLimit( limit );
            }

            if ( offset != null ) {
                queryBuilder.setSkip( offset );
            }

            if ( projection != null ) {
                queryBuilder.setProjection( Projection.newBuilder().putAllAttributes( projection ) );
            }

            if ( whereBuilder != null ) {
                queryBuilder.setWhere( whereBuilder.apply( parameterValues ) );
            }

            if ( knnBuilder != null ) {
                queryBuilder.setKnn( knnBuilder.apply( parameterValues ) );
            }*/

            queryResponseIterator = wrapper.query( QueryMessage.newBuilder().setQuery( query ).build() );

        } else {
            BatchedQueryMessage.Builder batchedQueryMessageBuilder = BatchedQueryMessage.newBuilder();
            for ( Map<Long, Object> parameterValues : dataContext.getParameterValues() ) {

                Integer limit = null;
                if ( limitBuilder != null ) {
                    limit = limitBuilder.apply( parameterValues );
                }

                Integer offset = null;
                if ( offsetBuilder != null ) {
                    offset = offsetBuilder.apply( parameterValues );
                }

                final Query query = buildSingleQuery( from, schema, projection, whereBuilder, knnBuilder, limit, offset, parameterValues );
                batchedQueryMessageBuilder.addQuery( query );
            }

            queryResponseIterator = wrapper.batchedQuery( batchedQueryMessageBuilder.build() );
        }

        return new CottontailQueryEnumerable<Object>( queryResponseIterator, rowParser );
    }


    private static Query buildSingleQuery(
            String from,
            String schema,
            Map<String, String> projection,
            Function1<Map<Long, Object>, Where> whereBuilder,
            Function1<Map<Long, Object>, Knn> knnBuilder, // TODO js(ct) FIGURE OUT
            Integer limit, Integer offset,
            Map<Long, Object> parameterValues
    ) {
        Query.Builder queryBuilder = Query.newBuilder();

        queryBuilder.setFrom( CottontailTypeUtil.fromFromTableAndSchema( from, schema ) );

        if ( limit != null ) {
            queryBuilder.setLimit( limit );
        }

        if ( offset != null ) {
            queryBuilder.setSkip( offset );
        }

        if ( projection != null ) {
            final Projection.Builder builder = Projection.newBuilder();
            for ( Entry<String, String> p : projection.entrySet() ) {
                builder.addColumns( ProjectionElement.newBuilder()
                        .setColumn( ColumnName.newBuilder().setName( p.getKey() ) )
                        .setAlias( ColumnName.newBuilder().setName( p.getValue() ) )
                );
            }
            queryBuilder.setProjection( builder );
        }

        if ( whereBuilder != null ) {
            queryBuilder.setWhere( whereBuilder.apply( parameterValues ) );
        }

        if ( knnBuilder != null ) {
            queryBuilder.setKnn( knnBuilder.apply( parameterValues ) );
        }

        return queryBuilder.build();
    }


    @Override
    public Enumerator<T> enumerator() {
        return new CottontailQueryResultEnumerator( this.queryIterator, this.rowParser );
    }


    private static class CottontailQueryResultEnumerator<T> implements Enumerator<T> {

        private final Function1<Map<String, Literal>, T> rowParser;
        private Iterator<QueryResponseMessage> queryIterator;

        private QueryResponseMessage currentQueryResponsePage;
        private Iterator<Tuple> currentResultIterator;
        private Map<String, CottontailGrpc.Literal> current = new HashMap<>();


        CottontailQueryResultEnumerator( Iterator<QueryResponseMessage> queryIterator, Function1<Map<String, Literal>, T> rowParser ) {
            this.queryIterator = queryIterator;
            this.rowParser = rowParser;
        }


        @Override
        public T current() {
            return this.rowParser.apply( this.current );
        }


        @Override
        public boolean moveNext() {
            /* Check if another QueryResponseMessage is waiting for processing and move the internal cursor forward, if so. */
            if ( this.currentResultIterator == null || !this.currentResultIterator.hasNext() ) {
                if ( this.queryIterator.hasNext() ) {
                    this.currentQueryResponsePage = this.queryIterator.next();
                    this.currentResultIterator = this.currentQueryResponsePage.getTuplesList().iterator();
                } else {
                    return false;
                }
            }

            if ( this.currentResultIterator.hasNext() ) {
                int i = 0;
                this.current.clear();
                for ( Literal l : this.currentResultIterator.next().getDataList() ) {
                    this.current.put( this.currentQueryResponsePage.getColumns( i++ ).getName(), l );
                }
                return true;
            } else {
                return false;
            }
        }


        @Override
        public void reset() {
            // TODO js(ct): do we need to do something here?
        }


        @Override
        public void close() {
            // TODO js(ct): do we need to do something here?
        }

    }


    public static class RowTypeParser implements Function1<Map<String, Literal>, Object[]> {

        private final RelDataType rowType;
        private final List<String> physicalColumnNames;


        public RowTypeParser( RelDataType rowType, List<String> physicalColumnNames ) {
            this.rowType = rowType;
            this.physicalColumnNames = physicalColumnNames;
        }


        @Override
        public Object[] apply( Map<String, Literal> a0 ) {
            Object[] returnValue = new Object[this.physicalColumnNames.size()];

            List<RelDataTypeField> fieldList = this.rowType.getFieldList();
            for ( int i = 0; i < fieldList.size(); i++ ) {
                RelDataType type = fieldList.get( i ).getType();
                String columnName = this.physicalColumnNames.get( i );
                returnValue[i] = this.parseSingleField( a0.get( columnName ), type );
            }

            return returnValue;
        }


        private Object parseSingleField( Literal data, RelDataType type ) {
            switch ( type.getPolyType() ) {
                case BOOLEAN:
                    return data.getBooleanData();
                case INTEGER:
                    return data.getIntData();
                case BIGINT:
                    return data.getLongData();
                case FLOAT:
                case REAL:
                    return data.getFloatData();
                case DOUBLE:
                    return data.getDoubleData();
                case CHAR:
                case VARCHAR:
                    return data.getStringData();
                case NULL:
                    return null;
                case DECIMAL:
                    return new BigDecimal( data.getStringData() );
                case BINARY:
                case VARBINARY:
                    return ByteString.parseBase64( data.getStringData() );
                case ARRAY:
                    ArrayType arrayType = (ArrayType) type;
                    if ( arrayType.getDimension() == 1 && CottontailToEnumerableConverter.SUPPORTED_ARRAY_COMPONENT_TYPES.contains( arrayType.getComponentType().getPolyType() ) ) {
                        switch ( arrayType.getComponentType().getPolyType() ) {
                            case INTEGER:
                                return data.getVectorData().getIntVector().getVectorList();
                            case BIGINT:
                                return data.getVectorData().getLongVector().getVectorList();
                            case DOUBLE:
                                return data.getVectorData().getDoubleVector().getVectorList();
                            case BOOLEAN:
                                return data.getVectorData().getBoolVector().getVectorList();
                            case FLOAT:
                            case REAL:
                                return data.getVectorData().getFloatVector().getVectorList();
                            default:
                                throw new RuntimeException( "Impossible to reach statement." );
                        }
                    } else {
                        SqlArrayValueConstructor.reparse( arrayType.getComponentType().getPolyType(), arrayType.getDimension(), data.getStringData() );
                    }
            }
            throw new AssertionError( "Not yet supported type: " + type.getPolyType() );
        }

    }

}
