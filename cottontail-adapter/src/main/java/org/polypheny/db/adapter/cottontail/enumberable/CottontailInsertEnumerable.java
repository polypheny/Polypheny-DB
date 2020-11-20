/*
 * Copyright 2019-2020 The Polypheny Project
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


import com.google.common.collect.ImmutableList;
import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.Collections;
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
import org.vitrivr.cottontail.grpc.CottontailGrpc;
import org.vitrivr.cottontail.grpc.CottontailGrpc.Entity;
import org.vitrivr.cottontail.grpc.CottontailGrpc.From;
import org.vitrivr.cottontail.grpc.CottontailGrpc.InsertMessage;
import org.vitrivr.cottontail.grpc.CottontailGrpc.Schema;
import org.vitrivr.cottontail.grpc.CottontailGrpc.Tuple;


public class CottontailInsertEnumerable<T> extends AbstractEnumerable<T> {

    public static final Method CREATE_INSERT_VALUES = Types.lookupMethod(
            CottontailInsertEnumerable.class,
            "fromValues",
            String.class, String.class, List.class, CottontailWrapper.class );


    public static final Method CREATE_INSERT_PREPARED = Types.lookupMethod(
            CottontailInsertEnumerable.class,
            "fromPreparedStatements",
            String.class, String.class, Function1.class, DataContext.class, CottontailWrapper.class );


    private List<InsertMessage> inserts;
    private DataContext dataContext;
    private CottontailWrapper wrapper;
    private boolean fromPrepared;


    public CottontailInsertEnumerable( List<InsertMessage> inserts, DataContext dataContext, CottontailWrapper wrapper, boolean fromPrepared ) {
        this.inserts = inserts;
        this.dataContext = dataContext;
        this.wrapper = wrapper;
        this.fromPrepared = fromPrepared;
    }


    @SuppressWarnings("unused") // Used via reflections
    public static CottontailInsertEnumerable<Object> fromValues(
            String from,
            String schema,
            List<Map<String, CottontailGrpc.Data>> values,
            CottontailWrapper wrapper
    ) {
        CottontailGrpc.From from_ = From.newBuilder().setEntity(
                Entity.newBuilder().setName( from ).setSchema(
                        Schema.newBuilder().setName( schema )
                ) ).build();

        List<InsertMessage> insertMessages = new ArrayList<>( values.size() );

        for ( Map<String, CottontailGrpc.Data> value : values ) {
            insertMessages.add( InsertMessage.newBuilder().setFrom( from_ ).setTuple(
                    Tuple.newBuilder().putAllData( value ).build()
            ).build() );
        }

        return new CottontailInsertEnumerable<>( insertMessages, null, wrapper, false );
    }


    @SuppressWarnings("unused") // Used via reflections
    public static CottontailInsertEnumerable<Object> fromPreparedStatements(
            String from,
            String schema,
            Function1<Map<Long, Object>, Map<String, CottontailGrpc.Data>> tupleBuilder,
            DataContext dataContext,
            CottontailWrapper wrapper
    ) {
        CottontailGrpc.From from_ = CottontailTypeUtil.fromFromTableAndSchema( from, schema );
        List<InsertMessage> insertMessages = new ArrayList<>();

        if ( dataContext.getParameterValues().size() == 0 ) {
            Map<Long, Object> parameterValues = new HashMap<>();
            Tuple tuple = Tuple.newBuilder().putAllData( tupleBuilder.apply( parameterValues ) ).build();
            insertMessages.add(
                    InsertMessage.newBuilder().setFrom( from_ ).setTuple( tuple ).build() );
        } else {
            for ( Map<Long, Object> parameterValues : dataContext.getParameterValues() ) {
                Tuple tuple = Tuple.newBuilder().putAllData( tupleBuilder.apply( parameterValues ) ).build();
                insertMessages.add(
                        InsertMessage.newBuilder().setFrom( from_ ).setTuple( tuple ).build() );
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
        private Iterator<InsertMessage> insertMessageIterator;

        private boolean wasSuccessful;
        private boolean executed;
        private long checkCount;
        private boolean fromPrepared;


        public CottontailInsertResultEnumerator( List<InsertMessage> inserts, CottontailWrapper wrapper, boolean fromPrepared ) {
            this.inserts = inserts;
            this.wrapper = wrapper;
            this.insertMessageIterator = inserts.iterator();
            this.checkCount = 0;
            this.fromPrepared = fromPrepared;
        }


        @SuppressWarnings("unchecked")
        @Override
        public T current() {
            if ( this.wasSuccessful ) {
                return this.fromPrepared ? (T) Integer.valueOf( 1 ) : (T) Integer.valueOf( this.inserts.size() );
            } else {
                return (T) Integer.valueOf( -1 );
            }
        }


        @Override
        public boolean moveNext() {

            if ( !this.executed ) {
//            if ( this.insertMessageIterator.hasNext() ) {
                this.wasSuccessful = this.wrapper.insert( this.inserts );
//                this.wasSuccessful = this.wrapper.insert( ImmutableList.of( this.insertMessageIterator.next() ) );
                executed = true;
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
