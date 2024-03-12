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

package org.polypheny.db.adapter.cottontail.enumberable;

import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import org.apache.calcite.linq4j.AbstractEnumerable;
import org.apache.calcite.linq4j.function.Function1;
import org.apache.calcite.linq4j.tree.Types;
import org.polypheny.db.adapter.DataContext;
import org.polypheny.db.adapter.cottontail.CottontailEntity;
import org.polypheny.db.adapter.cottontail.CottontailWrapper;
import org.polypheny.db.adapter.cottontail.util.CottontailTypeUtil;
import org.polypheny.db.catalog.exceptions.GenericRuntimeException;
import org.polypheny.db.type.entity.PolyValue;
import org.vitrivr.cottontail.client.iterators.Tuple;
import org.vitrivr.cottontail.client.iterators.TupleIterator;
import org.vitrivr.cottontail.client.language.basics.Constants;
import org.vitrivr.cottontail.grpc.CottontailGrpc;
import org.vitrivr.cottontail.grpc.CottontailGrpc.BatchInsertMessage;
import org.vitrivr.cottontail.grpc.CottontailGrpc.BatchInsertMessage.Insert;
import org.vitrivr.cottontail.grpc.CottontailGrpc.BatchedQueryMessage;
import org.vitrivr.cottontail.grpc.CottontailGrpc.ColumnName;
import org.vitrivr.cottontail.grpc.CottontailGrpc.Expression;
import org.vitrivr.cottontail.grpc.CottontailGrpc.InsertMessage;
import org.vitrivr.cottontail.grpc.CottontailGrpc.InsertMessage.InsertElement;
import org.vitrivr.cottontail.grpc.CottontailGrpc.Literal;
import org.vitrivr.cottontail.grpc.CottontailGrpc.Metadata;
import org.vitrivr.cottontail.grpc.CottontailGrpc.Order;
import org.vitrivr.cottontail.grpc.CottontailGrpc.Projection;
import org.vitrivr.cottontail.grpc.CottontailGrpc.Projection.ProjectionElement;
import org.vitrivr.cottontail.grpc.CottontailGrpc.Query;
import org.vitrivr.cottontail.grpc.CottontailGrpc.QueryMessage;
import org.vitrivr.cottontail.grpc.CottontailGrpc.UpdateMessage;
import org.vitrivr.cottontail.grpc.CottontailGrpc.UpdateMessage.UpdateElement;
import org.vitrivr.cottontail.grpc.CottontailGrpc.Where;


public class CottontailEnumerableFactory {

    /**
     * Method signature for building SELECT statements.
     */
    public static final Method CREATE_QUERY_METHOD = Types.lookupMethod(
            CottontailEnumerableFactory.class,
            "query",
            String.class, String.class, Map.class, Map.class, Function1.class, Function1.class, Function1.class, DataContext.class, Function1.class, CottontailEntity.class );

    /**
     * Method signature for building INSERT of values.
     */
    public static final Method CREATE_INSERT_VALUES = Types.lookupMethod(
            CottontailEnumerableFactory.class,
            "insertFromValues",
            String.class, String.class, List.class, DataContext.class, CottontailEntity.class );

    /**
     * Method signature for building INSERT for prepared statements.
     */
    public static final Method CREATE_INSERT_PREPARED = Types.lookupMethod(
            CottontailEnumerableFactory.class,
            "insertFromPreparedStatements",
            String.class, String.class, Function1.class, DataContext.class, CottontailEntity.class );

    /**
     * Method signature for building UPDATE with values.
     */
    public static final Method CREATE_UPDATE_METHOD = Types.lookupMethod(
            CottontailEnumerableFactory.class,
            "updateFromValues",
            String.class, String.class, Function1.class, Function1.class, DataContext.class, CottontailEntity.class );


    @SuppressWarnings("unused")
    public static CottontailQueryEnumerable query(
            String from,
            String schema,
            Map<Object, String> projection,
            Map<String, String> orderBy,
            Function1<Map<Long, PolyValue>, PolyValue> limitBuilder,
            Function1<Map<Long, PolyValue>, PolyValue> offsetBuilder,
            Function1<Map<Long, PolyValue>, Where> whereBuilder,
            DataContext dataContext,
            Function1<Tuple, PolyValue[]> rowParser,
            CottontailEntity entity
    ) {
        CottontailWrapper wrapper = entity.getCottontailNamespace().getWrapper();
        /* Begin or continue Cottontail DB transaction. */
        final long txId = wrapper.beginOrContinue( dataContext.getStatement().getTransaction() );

        /* Build SELECT messages and create enumerable. */
        TupleIterator queryResponseIterator;
        if ( dataContext.getParameterValues().size() < 2 ) {
            final Map<Long, PolyValue> parameterValues;
            if ( dataContext.getParameterValues().isEmpty() ) {
                parameterValues = new HashMap<>();
            } else {
                parameterValues = dataContext.getParameterValues().get( 0 );
            }

            Integer limit = null;
            if ( limitBuilder != null ) {
                limit = limitBuilder.apply( parameterValues ).asNumber().intValue();
            }

            Integer offset = null;
            if ( offsetBuilder != null ) {
                offset = offsetBuilder.apply( parameterValues ).asNumber().intValue();
            }

            final Query query = buildSingleQuery( from, schema, projection, orderBy, limit, offset, whereBuilder, parameterValues );
            queryResponseIterator = wrapper.query( QueryMessage.newBuilder().setMetadata( Metadata.newBuilder().setTransactionId( txId ) ).setQuery( query ).build() );
        } else {
            BatchedQueryMessage.Builder batchedQueryMessageBuilder = BatchedQueryMessage.newBuilder().setMetadata( Metadata.newBuilder().setTransactionId( txId ) );
            for ( Map<Long, PolyValue> parameterValues : dataContext.getParameterValues() ) {

                Integer limit = null;
                if ( limitBuilder != null ) {
                    limit = limitBuilder.apply( parameterValues ).asNumber().intValue();
                }

                Integer offset = null;
                if ( offsetBuilder != null ) {
                    offset = offsetBuilder.apply( parameterValues ).asNumber().intValue();
                }

                final Query query = buildSingleQuery( from, schema, projection, orderBy, limit, offset, whereBuilder, parameterValues );
                batchedQueryMessageBuilder.addQuery( query );
            }

            queryResponseIterator = wrapper.batchedQuery( batchedQueryMessageBuilder.build() );
        }

        return new CottontailQueryEnumerable( queryResponseIterator, rowParser );
    }


    /**
     * Used via reflection
     */
    private static Query buildSingleQuery(
            String from,
            String schema,
            Map<Object, String> projection,
            Map<String, String> order,
            Integer limit,
            Integer offset,
            Function1<Map<Long, PolyValue>, Where> whereBuilder,
            Map<Long, PolyValue> parameterValues
    ) {
        Query.Builder queryBuilder = Query.newBuilder();

        queryBuilder.setFrom( CottontailTypeUtil.fromFromTableAndSchema( from, schema ) );

        if ( limit != null ) {
            queryBuilder.setLimit( limit );
        }

        if ( offset != null ) {
            queryBuilder.setSkip( offset );
        }

        // Parse and translate projection clause (if available).
        if ( projection != null && !projection.isEmpty() ) {
            final Projection.Builder projBuilder = queryBuilder.getProjectionBuilder();
            for ( Entry<Object, String> p : projection.entrySet() ) {
                final Object key = p.getKey();
                if ( key instanceof String ) {
                    projBuilder.addElementsBuilder().setColumn( ColumnName.newBuilder().setName( (String) key ) );
                } else if ( key instanceof Function1 ) {
                    projBuilder.addElements( ((Function1<Map<Long, PolyValue>, ProjectionElement>) key).apply( parameterValues ) );
                }
            }
        }

        // Add WHERE clause to query.
        if ( whereBuilder != null ) {
            queryBuilder.setWhere( whereBuilder.apply( parameterValues ) );
        }

        // Add ORDER BY to query.
        if ( order != null && !order.isEmpty() ) {
            final Order.Builder orderBuilder = queryBuilder.getOrderBuilder();
            for ( Entry<String, String> p : order.entrySet() ) {
                orderBuilder.addComponentsBuilder()
                        .setColumn( ColumnName.newBuilder().setName( p.getKey() ).build() )
                        .setDirection( Order.Direction.valueOf( p.getValue() ) );
            }
        }

        return queryBuilder.build();
    }


    @SuppressWarnings("unused") // Used via reflection
    public static AbstractEnumerable<PolyValue[]> insertFromValues(
            String from,
            String schema,
            List<Map<String, Literal>> values,
            DataContext dataContext,
            CottontailEntity entity
    ) {
        CottontailWrapper wrapper = entity.getCottontailNamespace().getWrapper();
        // Begin or continue Cottontail DB transaction.
        final long txId = wrapper.beginOrContinue( dataContext.getStatement().getTransaction() );

        // Build INSERT messages and create enumerable.
        final CottontailGrpc.From from_ = CottontailTypeUtil.fromFromTableAndSchema( from, schema );
        final List<InsertMessage> insertMessages = new ArrayList<>( values.size() );
        for ( Map<String, CottontailGrpc.Literal> value : values ) {
            final InsertMessage.Builder message = InsertMessage.newBuilder().setFrom( from_ ).setMetadata( Metadata.newBuilder().setTransactionId( txId ) );
            for ( Entry<String, Literal> e : value.entrySet() ) {
                message.addElements( InsertElement.newBuilder().setColumn( ColumnName.newBuilder().setName( e.getKey() ) ).setValue( e.getValue() ).build() );
            }
            insertMessages.add( message.build() );
        }

        return new CottontailInsertEnumerable( insertMessages, wrapper );
    }


    @SuppressWarnings("unused") // Used via reflection
    public static AbstractEnumerable<PolyValue[]> insertFromPreparedStatements(
            String from,
            String schema,
            Function1<Map<Long, PolyValue>, Map<String, CottontailGrpc.Literal>> tupleBuilder,
            DataContext dataContext,
            CottontailEntity entity
    ) {
        CottontailWrapper wrapper = entity.getCottontailNamespace().getWrapper();
        /* Begin or continue Cottontail DB transaction. */
        final long txId = wrapper.beginOrContinue( dataContext.getStatement().getTransaction() );

        /* Build INSERT messages and create enumerable. */
        final CottontailGrpc.From from_ = CottontailTypeUtil.fromFromTableAndSchema( from, schema );
        if ( dataContext.getParameterValues().isEmpty() ) {
            final List<InsertMessage> insertMessages = new LinkedList<>();
            final InsertMessage.Builder insert = InsertMessage.newBuilder().setFrom( from_ ).setMetadata( Metadata.newBuilder().setTransactionId( txId ) );
            final Map<String, Literal> values = tupleBuilder.apply( new HashMap<>() );
            for ( Entry<String, Literal> e : values.entrySet() ) {
                insert.addElements( InsertElement.newBuilder().setColumn( ColumnName.newBuilder().setName( e.getKey() ) ).setValue( e.getValue() ) );
            }
            insertMessages.add( insert.build() );
            return new CottontailInsertEnumerable( insertMessages, wrapper );
        } else {
            final List<BatchInsertMessage> insertMessages = new LinkedList<>();
            BatchInsertMessage.Builder builder = BatchInsertMessage.newBuilder().setFrom( from_ ).setMetadata( Metadata.newBuilder().setTransactionId( txId ) );

            /* Add columns to BatchInsertMessage */
            final List<Map<Long, PolyValue>> parameterValues = dataContext.getParameterValues();
            for ( Entry<String, Literal> e : tupleBuilder.apply( parameterValues.get( 0 ) ).entrySet() ) {
                final ColumnName name = ColumnName.newBuilder().setName( e.getKey() ).build();
                builder.addColumns( name );
            }

            /* Start to track message size. */
            final int basicSize = builder.clone().build().getSerializedSize();
            int messageSize = basicSize;

            /* Add values to BatchInsertMessage. */
            for ( Map<Long, PolyValue> row : parameterValues ) {
                final Insert.Builder insertBuilder = Insert.newBuilder();
                for ( Entry<String, Literal> e : tupleBuilder.apply( row ).entrySet() ) {
                    insertBuilder.addValues( e.getValue() );
                }
                final Insert insert = insertBuilder.build();

                /* Check if maximum message size is exceeded. If so, build and add BatchInsertMessage to list. */
                if ( messageSize + insert.getSerializedSize() >= Constants.MAX_PAGE_SIZE_BYTES ) {
                    insertMessages.add( builder.build() );
                    builder = builder.clone().clearInserts();
                    messageSize = basicSize;
                }
                messageSize += insert.getSerializedSize();
                builder.addInserts( insert );
            }

            /* Add file message. */
            if ( builder.getInsertsCount() > 0 ) {
                insertMessages.add( builder.build() );
            }

            return new CottontailBatchInsertEnumerable( insertMessages, wrapper );
        }
    }


    /**
     * Builds and returns an {@link CottontailUpdateEnumerable}.
     */
    @SuppressWarnings("unused") // Used via reflection
    public static CottontailUpdateEnumerable updateFromValues(
            String entity,
            String schema,
            Function1<Map<Long, PolyValue>, Where> whereBuilder,
            Function1<Map<Long, PolyValue>, Map<String, CottontailGrpc.Literal>> tupleBuilder,
            DataContext dataContext,
            CottontailEntity cottontail
    ) {
        CottontailWrapper wrapper = cottontail.getCottontailNamespace().getWrapper();
        /* Begin or continue Cottontail DB transaction. */
        final long txId = wrapper.beginOrContinue( dataContext.getStatement().getTransaction() );

        /* Build UPDATE messages and create enumerable. */
        List<UpdateMessage> updateMessages;
        if ( dataContext.getParameterValues().size() < 2 ) {
            Map<Long, PolyValue> parameterValues;
            if ( dataContext.getParameterValues().isEmpty() ) {
                parameterValues = new HashMap<>();
            } else {
                parameterValues = dataContext.getParameterValues().get( 0 );
            }
            updateMessages = new ArrayList<>( 1 );
            updateMessages.add( buildSingleUpdate( entity, schema, txId, whereBuilder, tupleBuilder, parameterValues ) );
        } else {
            updateMessages = new ArrayList<>();
            for ( Map<Long, PolyValue> parameterValues : dataContext.getParameterValues() ) {
                updateMessages.add( buildSingleUpdate( entity, schema, txId, whereBuilder, tupleBuilder, parameterValues ) );
            }
        }

        return new CottontailUpdateEnumerable( updateMessages, wrapper );
    }


    private static UpdateMessage buildSingleUpdate(
            String entity,
            String schema,
            Long txId,
            Function1<Map<Long, PolyValue>, Where> whereBuilder,
            Function1<Map<Long, PolyValue>, Map<String, CottontailGrpc.Literal>> tupleBuilder,
            Map<Long, PolyValue> parameterValues
    ) {
        final UpdateMessage.Builder builder = UpdateMessage.newBuilder().setMetadata( Metadata.newBuilder().setTransactionId( txId ).build() );
        final CottontailGrpc.From from_ = CottontailTypeUtil.fromFromTableAndSchema( entity, schema );

        if ( whereBuilder != null ) {
            builder.setWhere( whereBuilder.apply( parameterValues ) );
        }

        try {
            for ( Entry<String, Literal> e : tupleBuilder.apply( parameterValues ).entrySet() ) {
                builder.addUpdates( UpdateElement.newBuilder()
                        .setColumn( ColumnName.newBuilder().setName( e.getKey() ) )
                        .setValue( Expression.newBuilder().setLiteral( e.getValue() ) )
                        .build() );
            }
        } catch ( RuntimeException e ) {
            throw new GenericRuntimeException( e );
        }

        builder.setFrom( from_ );

        return builder.build();
    }


    @SuppressWarnings("unused")
    public static PolyValue getDynamicValue( DataContext dataContext, long index ) {
        return dataContext.getParameterValues().get( 0 ).get( index );
    }

}
