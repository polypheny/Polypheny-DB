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

package org.polypheny.db.processing;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.TimeZone;
import lombok.Getter;
import lombok.Setter;
import org.apache.calcite.avatica.AvaticaSite;
import org.apache.calcite.linq4j.QueryProvider;
import org.jetbrains.annotations.NotNull;
import org.polypheny.db.adapter.DataContext;
import org.polypheny.db.adapter.java.JavaTypeFactory;
import org.polypheny.db.algebra.type.AlgDataType;
import org.polypheny.db.catalog.exceptions.GenericRuntimeException;
import org.polypheny.db.catalog.snapshot.Snapshot;
import org.polypheny.db.runtime.Hook;
import org.polypheny.db.transaction.Statement;
import org.polypheny.db.type.entity.PolyValue;
import org.polypheny.db.util.Holder;


/**
 * Implementation of DataContext.
 */
public class DataContextImpl implements DataContext {

    private final Map<String, Object> map;

    @Getter
    private final Snapshot snapshot;
    @Getter
    private final QueryProvider queryProvider;
    @Getter
    private final JavaTypeFactory typeFactory;
    private final TimeZone timeZone = TimeZone.getDefault();
    @Getter
    private final Statement statement;

    @Getter
    private Map<Long, AlgDataType> parameterTypes; // ParameterIndex -> Data ExpressionType
    @Getter
    private List<Map<Long, PolyValue>> parameterValues; // List of ( ParameterIndex -> Value )

    private final Map<Integer, List<Map<Long, PolyValue>>> otherParameterValues;

    int i = 0;

    @Getter
    @Setter
    private boolean isMixedModel = false;


    private DataContextImpl( QueryProvider queryProvider, Map<String, Object> parameters, Snapshot snapshot, JavaTypeFactory typeFactory, Statement statement, Map<Long, AlgDataType> parameterTypes, List<Map<Long, PolyValue>> parameterValues ) {
        this.queryProvider = queryProvider;
        this.typeFactory = typeFactory;
        this.snapshot = snapshot;
        this.statement = statement;
        this.map = getMedaInfo( parameters );
        this.parameterTypes = parameterTypes;
        this.parameterValues = new ArrayList<>( parameterValues );
        otherParameterValues = new HashMap<>();
    }


    public DataContextImpl( QueryProvider queryProvider, Map<String, Object> parameters, Snapshot snapshot, JavaTypeFactory typeFactory, Statement statement ) {
        this( queryProvider, parameters, snapshot, typeFactory, statement, new HashMap<>(), new ArrayList<>() );
    }


    @NotNull
    private Map<String, Object> getMedaInfo( Map<String, Object> parameters ) {
        // Store the time at which the query started executing. The SQL standard says that functions such as CURRENT_TIMESTAMP return the same value throughout the query.
        final Holder<Long> timeHolder = Holder.of( System.currentTimeMillis() );

        // Give a hook chance to alter the clock.
        Hook.CURRENT_TIME.run( timeHolder );
        final long time = timeHolder.get();
        final long localOffset = timeZone.getOffset( time );

        // Give a hook chance to alter standard input, output, error streams.
        final Holder<Object[]> streamHolder = Holder.of( new Object[]{ System.in, System.out, System.err } );
        Hook.STANDARD_STREAMS.run( streamHolder );

        Map<String, Object> map = new HashMap<>();
        for ( Map.Entry<String, Object> entry : parameters.entrySet() ) {
            Object e = entry.getValue();
            //e = AvaticaSite.DUMMY_VALUE;
            map.put( entry.getKey(), e );
        }
        return map;
    }


    @Override
    public synchronized Object get( String name ) {
        Object o = map.get( name );
        if ( o == AvaticaSite.DUMMY_VALUE ) {
            return null;
        }
        return o;
    }


    @Override
    public void addAll( Map<String, Object> map ) {
        this.map.putAll( map );
    }


    @Override
    public void addParameterValues( long index, AlgDataType type, List<PolyValue> data ) {
        if ( parameterTypes.containsKey( index ) ) {
            throw new GenericRuntimeException( "There are already values assigned to this index" );
        }
        if ( parameterValues.isEmpty() ) {
            for ( Object d : data ) {
                parameterValues.add( new HashMap<>() );
            }
        }
        if ( parameterValues.size() != data.size() ) {
            throw new GenericRuntimeException( "Expecting " + parameterValues.size() + " rows but " + data.size() + " values specified!" );
        }
        parameterTypes.put( index, type );
        int i = 0;
        for ( PolyValue d : data ) {
            parameterValues.get( i++ ).put( index, d );
        }
    }


    @Override
    public AlgDataType getParameterType( long index ) {
        return parameterTypes.get( index );
    }


    @Override
    public void setParameterValues( List<Map<Long, PolyValue>> values ) {
        parameterValues = new ArrayList<>( values );
    }


    @Override
    public void setParameterTypes( Map<Long, AlgDataType> types ) {
        parameterTypes = new HashMap<>( types );
    }


    @Override
    public void resetParameterValues() {
        parameterTypes = new HashMap<>();
        parameterValues = new ArrayList<>();
    }


    @Override
    public DataContext switchContext() {
        if ( otherParameterValues.containsKey( i ) ) {
            return new DataContextImpl( queryProvider, map, snapshot, typeFactory, statement, parameterTypes, otherParameterValues.get( i++ ) );
        }
        return this;
    }


    @Override
    public void addContext() {
        otherParameterValues.put( otherParameterValues.size(), parameterValues );
        parameterValues = new ArrayList<>();
    }


    @Override
    public void resetContext() {
        i = 0;
        if ( !otherParameterValues.isEmpty() ) {
            parameterValues = otherParameterValues.get( i );
        } else {
            parameterValues = new ArrayList<>();
        }
    }


}
