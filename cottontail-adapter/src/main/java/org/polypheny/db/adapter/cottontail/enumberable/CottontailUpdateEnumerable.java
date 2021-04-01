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
import java.util.Map.Entry;
import lombok.extern.slf4j.Slf4j;
import org.apache.calcite.linq4j.AbstractEnumerable;
import org.apache.calcite.linq4j.Enumerator;
import org.apache.calcite.linq4j.function.Function1;
import org.apache.calcite.linq4j.tree.Types;
import org.polypheny.db.adapter.DataContext;
import org.polypheny.db.adapter.cottontail.CottontailWrapper;
import org.polypheny.db.adapter.cottontail.util.CottontailTypeUtil;
import org.vitrivr.cottontail.grpc.CottontailGrpc;
import org.vitrivr.cottontail.grpc.CottontailGrpc.ColumnName;
import org.vitrivr.cottontail.grpc.CottontailGrpc.Literal;
import org.vitrivr.cottontail.grpc.CottontailGrpc.UpdateMessage;
import org.vitrivr.cottontail.grpc.CottontailGrpc.UpdateMessage.UpdateElement;
import org.vitrivr.cottontail.grpc.CottontailGrpc.Where;


@Slf4j
public class CottontailUpdateEnumerable<T> extends AbstractEnumerable<T> {

    public static final Method CREATE_UPDATE_METHOD = Types.lookupMethod(
            CottontailUpdateEnumerable.class,
            "update",
            String.class, String.class, Function1.class, Function1.class, DataContext.class, CottontailWrapper.class );

    private final List<UpdateMessage> updates;
    private final CottontailWrapper wrapper;


    public CottontailUpdateEnumerable( List<UpdateMessage> updates, DataContext dataContext, CottontailWrapper wrapper ) {
        this.updates = updates;
        this.wrapper = wrapper;
    }


    public static CottontailUpdateEnumerable<Object> update(
            String entity,
            String schema,
            Function1<Map<Long, Object>, Where> whereBuilder,
            Function1<Map<Long, Object>, Map<String, CottontailGrpc.Literal>> tupleBuilder,
            DataContext dataContext,
            CottontailWrapper wrapper
    ) {
        List<UpdateMessage> updateMessages;
        if ( dataContext.getParameterValues().size() < 2 ) {
            Map<Long, Object> parameterValues;
            if ( dataContext.getParameterValues().size() == 0 ) {
                parameterValues = new HashMap<>();
            } else {
                parameterValues = dataContext.getParameterValues().get( 0 );
            }
            updateMessages = new ArrayList<>( 1 );
            updateMessages.add( buildSingleUpdate( entity, schema, whereBuilder, tupleBuilder, parameterValues ) );
        } else {
            updateMessages = new ArrayList<>();
            for ( Map<Long, Object> parameterValues : dataContext.getParameterValues() ) {
                updateMessages.add( buildSingleUpdate( entity, schema, whereBuilder, tupleBuilder, parameterValues ) );
            }
        }

        return new CottontailUpdateEnumerable<>( updateMessages, dataContext, wrapper );
    }


    private static UpdateMessage buildSingleUpdate(
            String entity,
            String schema,
            Function1<Map<Long, Object>, Where> whereBuilder,
            Function1<Map<Long, Object>, Map<String, CottontailGrpc.Literal>> tupleBuilder,
            Map<Long, Object> parameterValues
    ) {
        UpdateMessage.Builder builder = UpdateMessage.newBuilder();

        CottontailGrpc.From from_ = CottontailTypeUtil.fromFromTableAndSchema( entity, schema );

        if ( whereBuilder != null ) {
            builder.setWhere( whereBuilder.apply( parameterValues ) );
        }

        try {
            for ( Entry<String, Literal> e : tupleBuilder.apply( parameterValues ).entrySet() ) {
                builder.addUpdates( UpdateElement.newBuilder()
                        .setColumn( ColumnName.newBuilder().setName( e.getKey() ) )
                        .setValue( e.getValue() )
                        .build() );
            }
        } catch ( RuntimeException e ) {
            log.error( "Something went wrong here!", e );
            throw new RuntimeException( e );
        }

        builder.setFrom( from_ );

        return builder.build();
    }


    @Override
    public Enumerator<T> enumerator() {
        return new CottontailUpdateEnumerator<>( updates, wrapper );
    }


    private static class CottontailUpdateEnumerator<T> implements Enumerator<T> {

        Iterator<UpdateMessage> updateMessageIterator;
        Long currentResult;
        CottontailWrapper wrapper;


        public CottontailUpdateEnumerator( List<UpdateMessage> updateMessageList, CottontailWrapper wrapper ) {
            this.updateMessageIterator = updateMessageList.iterator();
            this.wrapper = wrapper;
        }


        @Override
        public T current() {
            return (T) this.currentResult;
        }


        @Override
        public boolean moveNext() {
            if ( updateMessageIterator.hasNext() ) {
                UpdateMessage updateMessage = updateMessageIterator.next();

                this.currentResult = wrapper.update( updateMessage );

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
