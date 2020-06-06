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
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import org.polypheny.db.catalog.Catalog.IndexType;
import org.polypheny.db.catalog.entity.CatalogSchema;
import org.polypheny.db.catalog.entity.CatalogTable;
import org.polypheny.db.rex.RexLiteral;


public class HashIndex extends Index {


    private Set<List<RexLiteral>> index = new HashSet<>();


    public HashIndex(
            final boolean unique,
            final CatalogSchema schema,
            final CatalogTable table,
            final List<String> columns ) {
        this.type = IndexType.HASH;
        this.unique = unique;
        this.schema = schema;
        this.table = table;
        this.columns = ImmutableList.copyOf( columns );
    }

    public HashIndex(
            final boolean unique,
            final CatalogSchema schema,
            final CatalogTable table,
            final String... columns ) {
        this(unique, schema, table, Arrays.asList(columns));
    }


    @Override
    public boolean contains( List<RexLiteral> value ) {
        return index.contains( value );
    }


    @Override
    public boolean containsAny( Set<List<RexLiteral>> values ) {
        return index.contains( values );
    }


    @Override
    public boolean containsAll( Set<List<RexLiteral>> values ) {
        return index.containsAll( values );
    }


    @Override
    protected void clear() {
        index.clear();
    }


    @Override
    public void insert( List<RexLiteral> values ) {
        index.add( values );
    }


    @Override
    public void delete( List<RexLiteral> values ) {
        index.remove( values );
    }

}
