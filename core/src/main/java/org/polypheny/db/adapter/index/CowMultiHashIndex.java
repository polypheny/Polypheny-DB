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
import org.polypheny.db.catalog.entity.logical.LogicalNamespace;
import org.polypheny.db.catalog.entity.logical.LogicalTable;
import org.polypheny.db.rex.RexBuilder;
import org.polypheny.db.rex.RexLiteral;
import org.polypheny.db.tools.AlgBuilder;
import org.polypheny.db.transaction.PolyXid;
import org.polypheny.db.type.entity.PolyValue;
import org.polypheny.db.util.Pair;


public class CowMultiHashIndex extends Index {

    private final Map<List<PolyValue>, Set<List<PolyValue>>> index = new HashMap<>();
    private boolean initialized = false;

    private final Map<PolyXid, Map<List<PolyValue>, Set<List<PolyValue>>>> cowIndex = new HashMap<>();
    private final Map<PolyXid, List<DeferredIndexUpdate>> cowOpLog = new HashMap<>();
    private final Map<PolyXid, List<Triple<List<PolyValue>, List<PolyValue>, Boolean>>> barrierIndex = new HashMap<>();


    public CowMultiHashIndex( long id, String name, LogicalNamespace schema, LogicalTable table, List<String> columns, List<String> targetColumns ) {
        this.id = id;
        this.name = name;
        this.schema = schema;
        this.table = table;
        this.columns = ImmutableList.copyOf( columns );
        this.targetColumns = ImmutableList.copyOf( targetColumns );
    }


    public CowMultiHashIndex( long id, String name, LogicalNamespace schema, LogicalTable table, String[] columns, String[] targetColumns ) {
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
        for ( final Triple<List<PolyValue>, List<PolyValue>, Boolean> tuple : barrierIndex.get( xid ) ) {
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
    public boolean contains( PolyXid xid, List<PolyValue> value ) {
        Map<List<PolyValue>, Set<List<PolyValue>>> idx;
        if ( (idx = cowIndex.get( xid )) != null ) {
            if ( idx.containsKey( value ) ) {
                return idx.get( value ).size() > 0;
            }
        }
        return index.get( value ) != null && index.get( value ).size() > 0;
    }


    @Override
    public boolean containsAny( PolyXid xid, Iterable<List<PolyValue>> values ) {
        for ( final List<PolyValue> tuple : values ) {
            if ( contains( xid, tuple ) ) {
                return true;
            }
        }
        return false;
    }


    @Override
    public boolean containsAll( PolyXid xid, Iterable<List<PolyValue>> values ) {
        for ( final List<PolyValue> tuple : values ) {
            if ( !contains( xid, tuple ) ) {
                return false;
            }
        }
        return true;
    }


    @Override
    public Values getAsValues( PolyXid xid, AlgBuilder builder, AlgDataType rowType ) {
        final Map<List<PolyValue>, Set<List<PolyValue>>> ci = cowIndex.get( xid );
        final RexBuilder rexBuilder = builder.getRexBuilder();
        final List<ImmutableList<RexLiteral>> tuples = new ArrayList<>( index.size() + (ci != null ? ci.size() : 0) );
        for ( List<PolyValue> tuple : index.keySet() ) {
            if ( ci != null && ci.containsKey( tuple ) ) {
                // Tuple was modified in CoW index
                continue;
            }
            tuples.add( makeRexRow( rowType, rexBuilder, tuple ) );
        }
        if ( ci != null ) {
            for ( Map.Entry<List<PolyValue>, Set<List<PolyValue>>> tuple : ci.entrySet() ) {
                for ( int c = 0; c < tuple.getValue().size(); ++c ) {
                    // Tuple was added in CoW index
                    tuples.add( makeRexRow( rowType, rexBuilder, tuple.getKey() ) );
                }
            }
        }
        return (Values) builder.values( ImmutableList.copyOf( tuples ), rowType ).build();
    }


    @Override
    public Values getAsValues( PolyXid xid, AlgBuilder builder, AlgDataType rowType, List<PolyValue> key ) {
        final Map<List<PolyValue>, Set<List<PolyValue>>> ci = cowIndex.get( xid );
        final RexBuilder rexBuilder = builder.getRexBuilder();
        final List<ImmutableList<RexLiteral>> tuples = new ArrayList<>();
        Set<List<PolyValue>> raw = index.get( key );
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
    Map<List<PolyValue>, Set<List<PolyValue>>> getRaw() {
        return index;
    }


    @Override
    public void insertAll( PolyXid xid, final Iterable<Pair<List<PolyValue>, List<PolyValue>>> values ) {
        begin( xid );
        List<DeferredIndexUpdate> log = cowOpLog.get( xid );
        for ( final Pair<List<PolyValue>, List<PolyValue>> row : values ) {
            _insert( xid, row.getKey(), row.getValue() );
        }
        log.add( DeferredIndexUpdate.createInsert( values ) );
    }


    @Override
    public void insert( PolyXid xid, List<PolyValue> key, List<PolyValue> primary ) {
        begin( xid );
        List<DeferredIndexUpdate> log = cowOpLog.get( xid );
        _insert( xid, key, primary );
        log.add( DeferredIndexUpdate.createInsert( Collections.singleton( new Pair<>( key, primary ) ) ) );
    }


    protected void _insert( PolyXid xid, List<PolyValue> key, List<PolyValue> primary ) {
        List<Triple<List<PolyValue>, List<PolyValue>, Boolean>> idx = barrierIndex.get( xid );
        idx.add( new ImmutableTriple<>( key, primary, true ) );
    }


    protected void postBarrier( PolyXid xid, List<PolyValue> key, List<PolyValue> primary, boolean insert ) {
        Map<List<PolyValue>, Set<List<PolyValue>>> idx = cowIndex.get( xid );

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
    void insert( List<PolyValue> key, List<PolyValue> primary ) {
        if ( !index.containsKey( key ) ) {
            index.put( key, new HashSet<>() );
        }
        index.get( key ).add( primary );
    }


    @Override
    public void delete( PolyXid xid, List<PolyValue> key ) {
        begin( xid );
        List<DeferredIndexUpdate> log = cowOpLog.get( xid );

        _delete( xid, key, null );
        log.add( DeferredIndexUpdate.createDelete( Collections.singleton( key ) ) );
    }


    @Override
    public void deletePrimary( PolyXid xid, List<PolyValue> key, List<PolyValue> primary ) {
        begin( xid );
        List<DeferredIndexUpdate> log = cowOpLog.get( xid );

        _delete( xid, key, primary );
        log.add( DeferredIndexUpdate.createDelete( Collections.singleton( key ) ) );
    }


    protected void _delete( PolyXid xid, List<PolyValue> key, List<PolyValue> primary ) {
        List<Triple<List<PolyValue>, List<PolyValue>, Boolean>> idx = barrierIndex.get( xid );
        idx.add( new ImmutableTriple<>( key, primary, false ) );
    }


    @Override
    public void deleteAllPrimary( PolyXid xid, final Iterable<Pair<List<PolyValue>, List<PolyValue>>> values ) {
        begin( xid );
        List<DeferredIndexUpdate> log = cowOpLog.get( xid );

        for ( final Pair<List<PolyValue>, List<PolyValue>> value : values ) {
            _delete( xid, value.left, value.right );
        }
        log.add( DeferredIndexUpdate.createDeletePrimary( values ) );
    }


    @Override
    public void deleteAll( PolyXid xid, final Iterable<List<PolyValue>> values ) {
        begin( xid );
        List<DeferredIndexUpdate> log = cowOpLog.get( xid );

        for ( final List<PolyValue> value : values ) {
            _delete( xid, value, null );
        }
        log.add( DeferredIndexUpdate.createDelete( values ) );
    }


    @Override
    void delete( List<PolyValue> key ) {
        index.remove( key );
    }


    @Override
    void deletePrimary( List<PolyValue> key, List<PolyValue> primary ) {
        final Set<List<PolyValue>> primaries = index.get( key );
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
                LogicalNamespace schema,
                LogicalTable table,
                List<String> columns,
                List<String> targetColumns ) {
            return new CowMultiHashIndex( id, name, schema, table, columns, targetColumns );
        }

    }

}
