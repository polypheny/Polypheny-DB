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

package org.polypheny.db.adapter;


import com.google.common.collect.ImmutableList;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.polypheny.db.catalog.Catalog.IndexType;
import org.polypheny.db.catalog.entity.CatalogSchema;
import org.polypheny.db.catalog.entity.CatalogTable;
import org.polypheny.db.rex.RexLiteral;


public class HashIndex extends Index {


    private Map<List<RexLiteral>, List<RexLiteral>> index = new HashMap<>();
    private Map<List<RexLiteral>, List<RexLiteral>> reverseIndex = new HashMap<>();


    public HashIndex(
            final long id,
            final String name,
            final boolean unique,
            final CatalogSchema schema,
            final CatalogTable table,
            final CatalogTable targetTable,
            final List<String> columns,
            final List<String> targetColumns ) {
        this.id = id;
        this.name = name;
        this.type = IndexType.HASH;
        this.unique = unique;
        this.schema = schema;
        this.table = table;
        this.targetTable = targetTable;
        this.columns = ImmutableList.copyOf( columns );
        this.targetColumns = ImmutableList.copyOf( targetColumns );
    }

    public HashIndex(
            final long id,
            final String name,
            final boolean unique,
            final CatalogSchema schema,
            final CatalogTable table,
            final CatalogTable targetTable,
            final String[] columns,
            final String[] targetColumns) {
        this(id, name, unique, schema, table, targetTable, Arrays.asList(columns), Arrays.asList(targetColumns));
    }


    @Override
    public boolean contains( List<Object> value ) {
        return index.containsKey( value );
    }


    @Override
    public boolean containsAny( Set<List<Object>> values ) {
        for (final List<Object> tuple : values) {
            if ( index.containsKey( tuple ) ) {
                return true;
            }
        }
        return false;
    }


    @Override
    public boolean containsAll( Set<List<Object>> values ) {
        for (final List<Object> tuple : values) {
            if ( !index.containsKey( tuple ) ) {
                return false;
            }
        }
        return true;
    }


    @Override
    public List<List<RexLiteral>> getAll() {
        return new ArrayList<>( index.keySet() );
    }


    @Override
    protected void clear() {
        index.clear();
    }


    @Override
    public void insert( List<RexLiteral> key, List<RexLiteral> primary ) {
        System.err.println(String.format( "INDEX [%s] INSERT %s -> %s", name, key, primary ));
        index.put( key, primary );
        System.err.println("  => " + key);
        reverseIndex.put( primary, key );
    }


    @Override
    public void delete( List<RexLiteral> values ) {
        System.err.println(String.format( "INDEX [%s] DELETE %s -> ?", name, values ));
        List<RexLiteral> primary = index.remove( values );
        System.err.println("  => " + primary);
        reverseIndex.remove( primary );
    }

    @Override
    public void reverseDelete( List<RexLiteral> values ) {
        System.err.println(String.format( "INDEX [%s] DELETE ? -> %s", name, values ));
        List<RexLiteral> key = reverseIndex.remove( values );
        System.err.println("  => " + key);
        index.remove( key );
    }
}
