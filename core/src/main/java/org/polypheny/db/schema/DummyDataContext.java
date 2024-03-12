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

package org.polypheny.db.schema;

import com.google.common.collect.ImmutableMap;
import java.util.List;
import java.util.Map;
import lombok.Getter;
import org.apache.calcite.linq4j.QueryProvider;
import org.polypheny.db.adapter.DataContext;
import org.polypheny.db.adapter.java.JavaTypeFactory;
import org.polypheny.db.algebra.type.AlgDataType;
import org.polypheny.db.catalog.snapshot.Snapshot;
import org.polypheny.db.prepare.JavaTypeFactoryImpl;
import org.polypheny.db.transaction.Statement;
import org.polypheny.db.type.entity.PolyValue;

/**
 * Dummy data context that has no variables.
 */
class DummyDataContext implements DataContext {

    @Getter
    private final Snapshot snapshot;
    private final ImmutableMap<String, Object> map;


    DummyDataContext( Snapshot snapshot ) {
        this.snapshot = snapshot;
        this.map = ImmutableMap.of();
    }


    @Override
    public JavaTypeFactory getTypeFactory() {
        //return connection.getTypeFactory();
        return new JavaTypeFactoryImpl(); // TODO MV: Potential bug
    }


    @Override
    public QueryProvider getQueryProvider() {
        return null; // TODO MV: potential bug
    }


    @Override
    public Object get( String name ) {
        return map.get( name );
    }


    @Override
    public void addAll( Map<String, Object> map ) {
        throw new UnsupportedOperationException( "This operation is not supported for " + getClass().getSimpleName() );
    }


    @Override
    public Statement getStatement() {
        return null;
    }


    @Override
    public void addParameterValues( long index, AlgDataType type, List<PolyValue> data ) {
        throw new UnsupportedOperationException( "This operation is not supported for " + getClass().getSimpleName() );
    }


    @Override
    public AlgDataType getParameterType( long index ) {
        throw new UnsupportedOperationException( "This operation is not supported for " + getClass().getSimpleName() );
    }


    @Override
    public List<Map<Long, PolyValue>> getParameterValues() {
        throw new UnsupportedOperationException( "This operation is not supported for " + getClass().getSimpleName() );
    }


    @Override
    public void setParameterValues( List<Map<Long, PolyValue>> values ) {
        throw new UnsupportedOperationException( "This operation is not supported for " + getClass().getSimpleName() );
    }


    @Override
    public Map<Long, AlgDataType> getParameterTypes() {
        throw new UnsupportedOperationException( "This operation is not supported for " + getClass().getSimpleName() );
    }


    @Override
    public void setParameterTypes( Map<Long, AlgDataType> types ) {
        throw new UnsupportedOperationException( "This operation is not supported for " + getClass().getSimpleName() );
    }

}
