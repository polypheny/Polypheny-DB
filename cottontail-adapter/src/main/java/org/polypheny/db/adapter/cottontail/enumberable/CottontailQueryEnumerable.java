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
import org.apache.calcite.avatica.util.ByteString;
import org.apache.calcite.linq4j.AbstractEnumerable;
import org.apache.calcite.linq4j.Enumerator;
import org.apache.calcite.linq4j.function.Function1;
import org.apache.calcite.linq4j.tree.Types;
import org.polypheny.db.adapter.DataContext;
import org.polypheny.db.adapter.cottontail.CottontailToEnumerableConverter;
import org.polypheny.db.adapter.cottontail.CottontailWrapper;
import org.polypheny.db.rel.type.RelDataType;
import org.polypheny.db.rel.type.RelDataTypeField;
import org.polypheny.db.sql.fun.SqlArrayValueConstructor;
import org.polypheny.db.type.ArrayType;
import org.vitrivr.cottontail.grpc.CottontailGrpc;
import org.vitrivr.cottontail.grpc.CottontailGrpc.BatchedQueryMessage;
import org.vitrivr.cottontail.grpc.CottontailGrpc.Data;
import org.vitrivr.cottontail.grpc.CottontailGrpc.Entity;
import org.vitrivr.cottontail.grpc.CottontailGrpc.From;
import org.vitrivr.cottontail.grpc.CottontailGrpc.Knn;
import org.vitrivr.cottontail.grpc.CottontailGrpc.Projection;
import org.vitrivr.cottontail.grpc.CottontailGrpc.Query;
import org.vitrivr.cottontail.grpc.CottontailGrpc.QueryMessage;
import org.vitrivr.cottontail.grpc.CottontailGrpc.QueryResponseMessage;
import org.vitrivr.cottontail.grpc.CottontailGrpc.Schema;
import org.vitrivr.cottontail.grpc.CottontailGrpc.Tuple;
import org.vitrivr.cottontail.grpc.CottontailGrpc.Where;


public class CottontailQueryEnumerable<T> extends AbstractEnumerable<T> {

    public static final Method CREATE_QUERY_METHOD = Types.lookupMethod(
            CottontailQueryEnumerable.class,
            "query",
            String.class, String.class, Map.class, Function1.class, Function1.class, Function1.class, Function1.class, DataContext.class, Function1.class, CottontailWrapper.class );


    private final Iterator<QueryResponseMessage> queryIterator;
    private final Function1<Map<String, Data>, T> rowParser;


    public CottontailQueryEnumerable( Iterator<QueryResponseMessage> queryIterator, Function1<Map<String, Data>, T> rowParser ) {
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
            Map<Long, Object> parameterValues;
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

            Query query = buildSingleQuery( from, schema, projection, whereBuilder, knnBuilder, limit, offset, parameterValues );

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

            queryResponseIterator = wrapper.query(
                    QueryMessage.newBuilder().setQuery( query ).build() );

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

                Query query = buildSingleQuery( from, schema, projection, whereBuilder, knnBuilder, limit, offset, parameterValues );
                batchedQueryMessageBuilder.addQueries( query );
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
        }

        return queryBuilder.build();
    }


    @Override
    public Enumerator<T> enumerator() {
        return new CottontailQueryResultEnumerator<>( this.queryIterator, this.rowParser );
    }


    private static class CottontailQueryResultEnumerator<T> implements Enumerator<T> {

        private final Function1<Map<String, Data>, T> rowParser;
        private Iterator<QueryResponseMessage> queryIterator;

        private T current;
        private QueryResponseMessage currentQueryResponsePage;
        private Iterator<Tuple> currentResultIterator;


        CottontailQueryResultEnumerator( Iterator<QueryResponseMessage> queryIterator, Function1<Map<String, Data>, T> rowParser ) {
            this.queryIterator = queryIterator;
            this.rowParser = rowParser;
        }


        @Override
        public T current() {
            return this.current;
        }


        @Override
        public boolean moveNext() {
            // If we have a current result iterator and that iterator has a next result, use that result.
            // We do this first as this is (hopefully) the most common case.
            if ( this.currentResultIterator != null && this.currentResultIterator.hasNext() ) {
                Map<String, CottontailGrpc.Data> dataMap = this.currentResultIterator.next().getDataMap();
                this.current = this.rowParser.apply( dataMap );
                return true;
            }

            // Update the current query response page and and result iterator until we find a page that has results.
            // This is required because sometimes cottontail returns a page with no results but later pages contain
            // additional results.
            while ( this.queryIterator.hasNext() ) {
                this.currentQueryResponsePage = this.queryIterator.next();
                this.currentResultIterator = this.currentQueryResponsePage.getResultsList().iterator();

                // If the new result page contains results we update the current item and return.
                if ( this.currentResultIterator.hasNext() ) {
                    this.current = this.rowParser.apply( this.currentResultIterator.next().getDataMap() );
                    return true;
                }

                // Otherwise we continue to search for a result page that contains actual results.
            }

            return false;
/*

            // If the current result iterator doesn't have a next element but we have more response pages.
            if ( this.currentResultIterator == null || !this.currentResultIterator.hasNext() ) {
                if ( this.queryIterator.hasNext() ) {
                    this.currentQueryResponsePage = this.queryIterator.next();
                    this.currentResultIterator = this.currentQueryResponsePage.getResultsList().iterator();
                } else {
                    return false;
                }
            }

            // Update the current result
            if ( this.currentResultIterator.hasNext() ) {
                this.current = this.rowParser.apply( this.currentResultIterator.next().getDataMap() );
                return true;
            }

            if ( this.queryIterator.hasNext() ) {
                this.currentQueryResponsePage = this.queryIterator.next();
                this.currentResultIterator = this.currentQueryResponsePage.getResultsList().iterator();
                if ( this.currentResultIterator.hasNext() ) {
                    this.current = this.rowParser.apply( this.currentResultIterator.next().getDataMap() );
                    return true;
                }
            }



            return false;*/
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


    public static class RowTypeParser implements Function1<Map<String, Data>, Object[]> {

        private final RelDataType rowType;
        private final List<String> physicalColumnNames;


        public RowTypeParser( RelDataType rowType, List<String> physicalColumnNames ) {
            this.rowType = rowType;
            this.physicalColumnNames = physicalColumnNames;
        }


        @Override
        public Object[] apply( Map<String, Data> a0 ) {
            Object[] returnValue = new Object[this.physicalColumnNames.size()];
//            List<Object> returnValue = new ArrayList<>( this.physicalColumnNames.size() );

            List<RelDataTypeField> fieldList = this.rowType.getFieldList();
            for ( int i = 0; i < fieldList.size(); i++ ) {
                RelDataType type = fieldList.get( i ).getType();
                String columnName = this.physicalColumnNames.get( i );
                returnValue[i] = this.parseSingleField( a0.get( columnName ), type );
            }

            return returnValue;
        }


        private Object parseSingleField( Data data, RelDataType type ) {
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
