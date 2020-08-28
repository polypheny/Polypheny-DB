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

package org.polypheny.db.adapter.index;


import com.google.common.collect.ImmutableList;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.polypheny.db.catalog.Catalog.IndexType;
import org.polypheny.db.catalog.entity.CatalogSchema;
import org.polypheny.db.catalog.entity.CatalogTable;
import org.polypheny.db.rel.core.Values;
import org.polypheny.db.rel.type.RelDataType;
import org.polypheny.db.rex.RexBuilder;
import org.polypheny.db.rex.RexLiteral;
import org.polypheny.db.tools.RelBuilder;
import org.polypheny.db.transaction.PolyXid;


public class CowMultiHashIndex extends CoWHashIndex {

    protected Map<List<Object>, List<List<Object>>> index = new HashMap<>();
    protected Map<PolyXid, Map<List<Object>, List<List<Object>>>> cowIndex = new HashMap<>();


    public CowMultiHashIndex( long id, String name, CatalogSchema schema, CatalogTable table, List<String> columns, List<String> targetColumns ) {
        super( id, name, schema, table, columns, targetColumns );
    }


    public CowMultiHashIndex( long id, String name, CatalogSchema schema, CatalogTable table, String[] columns, String[] targetColumns ) {
        super( id, name, schema, table, columns, targetColumns );
    }


    @Override
    public boolean isUnique() {
        return false;
    }



    @Override
    public boolean contains( PolyXid xid, List<Object> value ) {
        Map<List<Object>, List<List<Object>>> idx;
        if ( (idx = cowIndex.get( xid )) != null ) {
            if ( idx.containsKey( value ) ) {
                return idx.get( value ).size() > 0;
            }
        }
        return index.get( value ) != null && index.get( value ).size() > 0;
    }


    @Override
    public Values getAsValues( PolyXid xid, RelBuilder builder, RelDataType rowType ) {
        final Map<List<Object>, List<List<Object>>> ci = cowIndex.get( xid );
        final RexBuilder rexBuilder = builder.getRexBuilder();
        final List<ImmutableList<RexLiteral>> tuples = new ArrayList<>( index.size() + (ci != null ? ci.size() : 0) );
        for ( List<Object> tuple : index.keySet() ) {
            if ( ci != null && ci.containsKey( tuple ) ) {
                // Tuple was modified in CoW index
                continue;
            }
            tuples.add( makeRexRow( rowType, rexBuilder, tuple ) );
        }
        if ( ci != null ) {
            for ( Map.Entry<List<Object>, List<List<Object>>> tuple : ci.entrySet() ) {
                for ( int c = 0; c < tuple.getValue().size(); ++c ) {
                    // Tuple was added in CoW index
                    tuples.add( makeRexRow( rowType, rexBuilder, tuple.getKey() ) );
                }
            }
        }
        return (Values) builder.values( ImmutableList.copyOf( tuples ), rowType ).build();
    }


    @Override
    protected void _insert( PolyXid xid, List<Object> key, List<Object> primary ) {
        Map<List<Object>, List<List<Object>>> idx = cowIndex.get( xid );
        if (!idx.containsKey( key )) {
            idx.put( key, index.get( key ) );
        }
        idx.get( key ).add( primary );
    }


    @Override
    public void insert( List<Object> key, List<Object> primary ) {
        if (!index.containsKey( key )) {
            index.put( key, new ArrayList<>(  ) );
        }
        index.get( key ).add( primary );
    }


    @Override
    protected void _delete( PolyXid xid, List<Object> key ) {
        Map<List<Object>, List<List<Object>>> idx = cowIndex.get( xid );
        if (!idx.containsKey( key )) {
            idx.put( key, index.containsKey( key ) ? index.get( key ) : new ArrayList<>(  ) );
        }
        idx.get( key ).clear();
    }


    @Override
    void delete( List<Object> key ) {
        index.remove( key );
    }


    static class Factory implements IndexFactory {

        @Override
        public boolean isUnique() {
            return false;
        }


        @Override
        public IndexType getType() {
            return IndexType.HASH;
        }


        @Override
        public Index create( long id, String name, CatalogSchema schema, CatalogTable table, List<String> columns, List<String> targetColumns ) {
            return new CowMultiHashIndex( id, name, schema, table, columns, targetColumns );
        }

    }
}
