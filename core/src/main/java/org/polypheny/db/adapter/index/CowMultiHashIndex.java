/*
 * Copyright 2019-2022 The Polypheny Project
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
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.apache.commons.lang3.tuple.ImmutableTriple;
import org.apache.commons.lang3.tuple.Triple;
import org.polypheny.db.algebra.core.Values;
import org.polypheny.db.algebra.type.AlgDataType;
import org.polypheny.db.catalog.entity.CatalogSchema;
import org.polypheny.db.catalog.entity.CatalogTable;
import org.polypheny.db.rex.RexBuilder;
import org.polypheny.db.rex.RexLiteral;
import org.polypheny.db.tools.AlgBuilder;
import org.polypheny.db.transaction.PolyXid;
import org.polypheny.db.util.Pair;


public class CowMultiHashIndex extends Index {

    private Map<List<Object>, Set<List<Object>>> index = new HashMap<>();
    private boolean initialized = false;

    private Map<PolyXid, Map<List<Object>, Set<List<Object>>>> cowIndex = new HashMap<>();
    private Map<PolyXid, List<DeferredIndexUpdate>> cowOpLog = new HashMap<>();
    private Map<PolyXid, List<Triple<List<Object>, List<Object>, Boolean>>> barrierIndex = new HashMap<>();


    public CowMultiHashIndex( long id, String name, CatalogSchema schema, CatalogTable table, List<String> columns, List<String> targetColumns ) {
        this.id = id;
        this.name = name;
        this.schema = schema;
        this.table = table;
        this.columns = ImmutableList.copyOf( columns );
        this.targetColumns = ImmutableList.copyOf( targetColumns );
    }


    public CowMultiHashIndex( long id, String name, CatalogSchema schema, CatalogTable table, String[] columns, String[] targetColumns ) {
        this( id, name, schema, table, Arrays.asList( columns ), Arrays.asList( targetColumns ) );
    }


    @Override
    public String getMethod() {
        return "hash";
    }


    @Override
    public boolean isUnique() {
        return false;
    }


    @Override
    public boolean isPersistent() {
        return false;
    }


    @Override
    void commit( PolyXid xid ) {
        begin( xid );
        for ( final DeferredIndexUpdate update : this.cowOpLog.get( xid ) ) {
            update.execute( this );
        }
        rollback( xid );
    }


    @Override
    public void barrier( PolyXid xid ) {
        begin( xid );
        for ( final Triple<List<Object>, List<Object>, Boolean> tuple : barrierIndex.get( xid ) ) {
            postBarrier( xid, tuple.getLeft(), tuple.getMiddle(), tuple.getRight() );
        }
        barrierIndex.get( xid ).clear();
    }


    @Override
    void rollback( PolyXid xid ) {
        this.cowIndex.remove( xid );
        this.cowOpLog.remove( xid );
        this.barrierIndex.remove( xid );
    }


    @Override
    protected void clear() {
        index.clear();
        cowIndex.clear();
        cowOpLog.clear();
        barrierIndex.clear();
    }


    @Override
    boolean isInitialized() {
        return initialized;
    }


    @Override
    void initialize() {
        initialized = true;
    }


    @Override
    public int size() {
        return index.size();
    }


    protected void begin( PolyXid xid ) {
        if ( !cowIndex.containsKey( xid ) ) {
            IndexManager.getInstance().begin( xid, this );
            cowIndex.put( xid, new HashMap<>() );
            cowOpLog.put( xid, new ArrayList<>() );
            barrierIndex.put( xid, new ArrayList<>() );
        }
    }


    @Override
    public boolean contains( PolyXid xid, List<Object> value ) {
        Map<List<Object>, Set<List<Object>>> idx;
        if ( (idx = cowIndex.get( xid )) != null ) {
            if ( idx.containsKey( value ) ) {
                return idx.get( value ).size() > 0;
            }
        }
        return index.get( value ) != null && index.get( value ).size() > 0;
    }


    @Override
    public boolean containsAny( PolyXid xid, Iterable<List<Object>> values ) {
        for ( final List<Object> tuple : values ) {
            if ( contains( xid, tuple ) ) {
                return true;
            }
        }
        return false;
    }


    @Override
    public boolean containsAll( PolyXid xid, Iterable<List<Object>> values ) {
        for ( final List<Object> tuple : values ) {
            if ( !contains( xid, tuple ) ) {
                return false;
            }
        }
        return true;
    }


    @Override
    public Values getAsValues( PolyXid xid, AlgBuilder builder, AlgDataType rowType ) {
        final Map<List<Object>, Set<List<Object>>> ci = cowIndex.get( xid );
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
            for ( Map.Entry<List<Object>, Set<List<Object>>> tuple : ci.entrySet() ) {
                for ( int c = 0; c < tuple.getValue().size(); ++c ) {
                    // Tuple was added in CoW index
                    tuples.add( makeRexRow( rowType, rexBuilder, tuple.getKey() ) );
                }
            }
        }
        return (Values) builder.values( ImmutableList.copyOf( tuples ), rowType ).build();
    }


    @Override
    public Values getAsValues( PolyXid xid, AlgBuilder builder, AlgDataType rowType, List<Object> key ) {
        final Map<List<Object>, Set<List<Object>>> ci = cowIndex.get( xid );
        final RexBuilder rexBuilder = builder.getRexBuilder();
        final List<ImmutableList<RexLiteral>> tuples = new ArrayList<>();
        Set<List<Object>> raw = index.get( key );
        if ( ci != null && ci.containsKey( key ) ) {
            raw = ci.get( key );
        }
        if ( raw == null ) {
            return (Values) builder.values( ImmutableList.of(), rowType ).build();
        }
        for ( int i = 0; i < raw.size(); ++i ) {
            tuples.add( makeRexRow( rowType, rexBuilder, key ) );
        }
        return (Values) builder.values( ImmutableList.copyOf( tuples ), rowType ).build();
    }


    @Override
    Map<List<Object>, Set<List<Object>>> getRaw() {
        return index;
    }


    @Override
    public void insertAll( PolyXid xid, final Iterable<Pair<List<Object>, List<Object>>> values ) {
        begin( xid );
        List<DeferredIndexUpdate> log = cowOpLog.get( xid );
        for ( final Pair<List<Object>, List<Object>> row : values ) {
            _insert( xid, row.getKey(), row.getValue() );
        }
        log.add( DeferredIndexUpdate.createInsert( values ) );
    }


    @Override
    public void insert( PolyXid xid, List<Object> key, List<Object> primary ) {
        begin( xid );
        List<DeferredIndexUpdate> log = cowOpLog.get( xid );
        _insert( xid, key, primary );
        log.add( DeferredIndexUpdate.createInsert( Collections.singleton( new Pair<>( key, primary ) ) ) );
    }


    protected void _insert( PolyXid xid, List<Object> key, List<Object> primary ) {
        List<Triple<List<Object>, List<Object>, Boolean>> idx = barrierIndex.get( xid );
        idx.add( new ImmutableTriple<>( key, primary, true ) );
    }


    protected void postBarrier( PolyXid xid, List<Object> key, List<Object> primary, boolean insert ) {
        Map<List<Object>, Set<List<Object>>> idx = cowIndex.get( xid );

        if ( !idx.containsKey( key ) ) {
            if ( index.containsKey( key ) ) {
                idx.put( key, index.get( key ) );
            } else {
                idx.put( key, new HashSet<>() );
            }
        }
        if ( insert ) {
            idx.get( key ).add( primary );
        } else {
            if ( primary == null ) {
                idx.get( key ).clear();
            } else {
                idx.get( key ).remove( primary );
            }
        }
    }


    @Override
    void insert( List<Object> key, List<Object> primary ) {
        if ( !index.containsKey( key ) ) {
            index.put( key, new HashSet<>() );
        }
        index.get( key ).add( primary );
    }


    @Override
    public void delete( PolyXid xid, List<Object> key ) {
        begin( xid );
        List<DeferredIndexUpdate> log = cowOpLog.get( xid );

        _delete( xid, key, null );
        log.add( DeferredIndexUpdate.createDelete( Collections.singleton( key ) ) );
    }


    @Override
    public void deletePrimary( PolyXid xid, List<Object> key, List<Object> primary ) {
        begin( xid );
        List<DeferredIndexUpdate> log = cowOpLog.get( xid );

        _delete( xid, key, primary );
        log.add( DeferredIndexUpdate.createDelete( Collections.singleton( key ) ) );
    }


    protected void _delete( PolyXid xid, List<Object> key, List<Object> primary ) {
        List<Triple<List<Object>, List<Object>, Boolean>> idx = barrierIndex.get( xid );
        idx.add( new ImmutableTriple<>( key, primary, false ) );
    }


    @Override
    public void deleteAllPrimary( PolyXid xid, final Iterable<Pair<List<Object>, List<Object>>> values ) {
        begin( xid );
        List<DeferredIndexUpdate> log = cowOpLog.get( xid );

        for ( final Pair<List<Object>, List<Object>> value : values ) {
            _delete( xid, value.left, value.right );
        }
        log.add( DeferredIndexUpdate.createDeletePrimary( values ) );
    }


    @Override
    public void deleteAll( PolyXid xid, final Iterable<List<Object>> values ) {
        begin( xid );
        List<DeferredIndexUpdate> log = cowOpLog.get( xid );

        for ( final List<Object> value : values ) {
            _delete( xid, value, null );
        }
        log.add( DeferredIndexUpdate.createDelete( values ) );
    }


    @Override
    void delete( List<Object> key ) {
        index.remove( key );
    }


    @Override
    void deletePrimary( List<Object> key, List<Object> primary ) {
        final Set<List<Object>> primaries = index.get( key );
        if ( primaries != null ) {
            primaries.remove( primary );
        }
    }


    static class Factory implements IndexFactory {

        @Override
        public boolean canProvide( String method, Boolean unique, Boolean persistent ) {
            return (method == null || method.equals( "hash" ))
                    && (unique == null || !unique)
                    && (persistent == null || !persistent);
        }


        @Override
        public Index create(
                long id,
                String name,
                String method,
                Boolean unique,
                Boolean persistent,
                CatalogSchema schema,
                CatalogTable table,
                List<String> columns,
                List<String> targetColumns ) {
            return new CowMultiHashIndex( id, name, schema, table, columns, targetColumns );
        }

    }

}
