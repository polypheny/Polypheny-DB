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
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import org.apache.calcite.linq4j.AbstractEnumerable;
import org.apache.calcite.linq4j.Enumerator;
import org.apache.calcite.linq4j.function.Function1;
import org.apache.calcite.linq4j.tree.Types;
import org.polypheny.db.adapter.DataContext;
import org.polypheny.db.adapter.cottontail.CottontailWrapper;
import org.polypheny.db.adapter.cottontail.util.CottontailTypeUtil;
import org.vitrivr.cottontail.grpc.CottontailGrpc;
import org.vitrivr.cottontail.grpc.CottontailGrpc.ColumnName;
import org.vitrivr.cottontail.grpc.CottontailGrpc.InsertMessage;
import org.vitrivr.cottontail.grpc.CottontailGrpc.InsertMessage.InsertElement;
import org.vitrivr.cottontail.grpc.CottontailGrpc.Literal;
import org.vitrivr.cottontail.grpc.CottontailGrpc.TransactionId;


public class CottontailInsertEnumerable<T> extends AbstractEnumerable<T> {

    /**
     * Method signature for INSERT of values.
     */
    public static final Method CREATE_INSERT_VALUES = Types.lookupMethod(
            CottontailInsertEnumerable.class,
            "fromValues",
            String.class, String.class, List.class, DataContext.class, CottontailWrapper.class );

    /**
     * Method signature for INSERT for prepared statements.
     */
    public static final Method CREATE_INSERT_PREPARED = Types.lookupMethod(
            CottontailInsertEnumerable.class,
            "fromPreparedStatements",
            String.class, String.class, Function1.class, DataContext.class, CottontailWrapper.class );

    private final List<InsertMessage> inserts;
    private final CottontailWrapper wrapper;
    private final boolean fromPrepared;


    public CottontailInsertEnumerable( List<InsertMessage> inserts, DataContext dataContext, CottontailWrapper wrapper, boolean fromPrepared ) {
        this.inserts = inserts;
        this.wrapper = wrapper;
        this.fromPrepared = fromPrepared;
    }


    @SuppressWarnings("unused") // Used via reflection
    public static CottontailInsertEnumerable<Object> fromValues(
            String from,
            String schema,
            List<Map<String, CottontailGrpc.Literal>> values,
            DataContext dataContext,
            CottontailWrapper wrapper
    ) {
        /* Begin or continue Cottontail DB transaction. */
        final TransactionId txId = wrapper.beginOrContinue( dataContext.getStatement().getTransaction() );

        /* Build INSERT messages and create enumerable. */
        final CottontailGrpc.From from_ = CottontailTypeUtil.fromFromTableAndSchema( from, schema );
        final List<InsertMessage> insertMessages = new ArrayList<>( values.size() );
        for ( Map<String, CottontailGrpc.Literal> value : values ) {
            final InsertMessage.Builder message = InsertMessage.newBuilder().setFrom( from_ ).setTxId( txId );
            for ( Entry<String, Literal> e : value.entrySet() ) {
                message.addElements( InsertElement.newBuilder().setColumn( ColumnName.newBuilder().setName( e.getKey() ) ).setValue( e.getValue() ).build() );
            }
            insertMessages.add( message.build() );
        }

        return new CottontailInsertEnumerable<>( insertMessages, null, wrapper, false );
    }


    @SuppressWarnings("unused") // Used via reflection
    public static CottontailInsertEnumerable<Object> fromPreparedStatements(
            String from,
            String schema,
            Function1<Map<Long, Object>, Map<String, CottontailGrpc.Literal>> tupleBuilder,
            DataContext dataContext,
            CottontailWrapper wrapper
    ) {
        /* Begin or continue Cottontail DB transaction. */
        final TransactionId txId = wrapper.beginOrContinue( dataContext.getStatement().getTransaction() );

        /* Build INSERT messages and create enumerable. */
        final CottontailGrpc.From from_ = CottontailTypeUtil.fromFromTableAndSchema( from, schema );
        final List<InsertMessage> insertMessages = new ArrayList<>();
        if ( dataContext.getParameterValues().size() == 0 ) {
            final InsertMessage.Builder insert = InsertMessage.newBuilder().setFrom( from_ ).setTxId( txId );
            final Map<String, Literal> values = tupleBuilder.apply( new HashMap<>() );
            for ( Entry<String, Literal> e : values.entrySet() ) {
                insert.addElements( InsertElement.newBuilder().setColumn( ColumnName.newBuilder().setName( e.getKey() ) ).setValue( e.getValue() ) );
            }
            insertMessages.add( insert.build() );
        } else {
            for ( Map<Long, Object> parameterValues : dataContext.getParameterValues() ) {
                final InsertMessage.Builder insert = InsertMessage.newBuilder().setFrom( from_ ).setTxId( txId );
                for ( Entry<String, CottontailGrpc.Literal> e : tupleBuilder.apply( parameterValues ).entrySet() ) {
                    insert.addElements( InsertElement.newBuilder().setColumn( ColumnName.newBuilder().setName( e.getKey() ) ).setValue( e.getValue() ) );
                }
                insertMessages.add( insert.build() );
            }
        }

        return new CottontailInsertEnumerable<>( insertMessages, dataContext, wrapper, true );
    }


    @Override
    public Enumerator<T> enumerator() {
        return new CottontailInsertResultEnumerator<>( inserts, wrapper, fromPrepared );
    }


    private static class CottontailInsertResultEnumerator<T> implements Enumerator<T> {

        private List<InsertMessage> inserts;
        private CottontailWrapper wrapper;

        private boolean wasSuccessful;
        private boolean executed;
        private long checkCount;
        private boolean fromPrepared;


        public CottontailInsertResultEnumerator( List<InsertMessage> inserts, CottontailWrapper wrapper, boolean fromPrepared ) {
            this.inserts = inserts;
            this.wrapper = wrapper;
            this.checkCount = 0;
            this.fromPrepared = fromPrepared;
        }


        @SuppressWarnings("unchecked")
        @Override
        public T current() {
            if ( this.wasSuccessful ) {
                return (T) Integer.valueOf( this.inserts.size() );
            } else {
                return (T) Integer.valueOf( -1 );
            }
        }


        @Override
        public boolean moveNext() {
            if ( !this.executed ) {
                this.wasSuccessful = this.wrapper.insert( this.inserts );
                this.executed = true;
                this.checkCount += 1;
                return this.wasSuccessful;
            } else {
                if ( !this.fromPrepared ) {
                    return false;
                }
                if ( this.checkCount < this.inserts.size() ) {
                    this.checkCount += 1;
                    return true;
                } else {
                    return false;
                }
            }
        }


        @Override
        public void reset() {
        }


        @Override
        public void close() {
        }

    }

}
