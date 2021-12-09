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
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import org.apache.calcite.linq4j.AbstractEnumerable;
import org.apache.calcite.linq4j.Enumerator;
import org.apache.calcite.linq4j.function.Function1;
import org.apache.calcite.linq4j.tree.Types;
import org.polypheny.db.adapter.DataContext;
import org.polypheny.db.adapter.cottontail.CottontailWrapper;
import org.polypheny.db.adapter.cottontail.util.CottontailTypeUtil;
import org.vitrivr.cottontail.grpc.CottontailGrpc.DeleteMessage;
import org.vitrivr.cottontail.grpc.CottontailGrpc.Metadata;
import org.vitrivr.cottontail.grpc.CottontailGrpc.Where;


public class CottontailDeleteEnumerable<T> extends AbstractEnumerable<T> {

    public static final Method CREATE_DELETE_METHOD = Types.lookupMethod(
            CottontailDeleteEnumerable.class,
            "delete",
            String.class, String.class, Function1.class, DataContext.class, CottontailWrapper.class );

    private final List<DeleteMessage> deletes;
    private final CottontailWrapper wrapper;


    public CottontailDeleteEnumerable( List<DeleteMessage> deletes, DataContext dataContext, CottontailWrapper wrapper ) {
        this.deletes = deletes;
        this.wrapper = wrapper;
    }


    @Override
    public Enumerator<T> enumerator() {
        return new CottontailDeleteEnumerator<>( deletes, wrapper );
    }


    public static CottontailDeleteEnumerable<Object> delete(
            String entity,
            String schema,
            Function1<Map<Long, Object>, Where> whereBuilder,
            DataContext dataContext,
            CottontailWrapper wrapper
    ) {
        /* Begin or continue Cottontail DB transaction. */
        final Long txId = wrapper.beginOrContinue( dataContext.getStatement().getTransaction() );

        /* Build DELETE messages and create enumerable. */
        List<DeleteMessage> deleteMessages;
        if ( dataContext.getParameterValues().size() < 2 ) {
            Map<Long, Object> parameterValues;
            if ( dataContext.getParameterValues().size() == 0 ) {
                parameterValues = new HashMap<>();
            } else {
                parameterValues = dataContext.getParameterValues().get( 0 );
            }
            deleteMessages = new ArrayList<>( 1 );
            deleteMessages.add( buildSingleDelete( entity, schema, txId, whereBuilder, parameterValues ) );
        } else {
            deleteMessages = new ArrayList<>();
            for ( Map<Long, Object> parameterValues : dataContext.getParameterValues() ) {
                deleteMessages.add( buildSingleDelete( entity, schema, txId, whereBuilder, parameterValues ) );
            }
        }

        return new CottontailDeleteEnumerable<>( deleteMessages, dataContext, wrapper );
    }


    private static DeleteMessage buildSingleDelete(
            String entity,
            String schema,
            Long txId,
            Function1<Map<Long, Object>, Where> whereBuilder,
            Map<Long, Object> parameterValues
    ) {
        final DeleteMessage.Builder builder = DeleteMessage.newBuilder().setMetadata( Metadata.newBuilder().setTransactionId( txId ).build() );
        builder.setFrom( CottontailTypeUtil.fromFromTableAndSchema( entity, schema ) );
        if ( whereBuilder != null ) {
            builder.setWhere( whereBuilder.apply( parameterValues ) );
        }

        return builder.build();
    }


    private static class CottontailDeleteEnumerator<T> implements Enumerator<T> {

        Iterator<DeleteMessage> deleteMessageIterator;
        Long currentResult;
        CottontailWrapper wrapper;


        public CottontailDeleteEnumerator( List<DeleteMessage> deleteMessages, CottontailWrapper wrapper ) {
            this.deleteMessageIterator = deleteMessages.iterator();
            this.wrapper = wrapper;
        }


        @Override
        public T current() {
            return (T) currentResult;
        }


        @Override
        public boolean moveNext() {
            if ( deleteMessageIterator.hasNext() ) {
                DeleteMessage deleteMessage = deleteMessageIterator.next();

                this.currentResult = wrapper.delete( deleteMessage );

                return !this.currentResult.equals( -1L );
            }
            return false;
        }


        @Override
        public void reset() {

        }


        @Override
        public void close() {

        }

    }

}
