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
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import lombok.extern.slf4j.Slf4j;
import org.polypheny.db.algebra.core.Values;
import org.polypheny.db.algebra.exceptions.ConstraintViolationException;
import org.polypheny.db.algebra.type.AlgDataType;
import org.polypheny.db.catalog.entity.CatalogSchema;
import org.polypheny.db.catalog.entity.CatalogTable;
import org.polypheny.db.rex.RexBuilder;
import org.polypheny.db.rex.RexLiteral;
import org.polypheny.db.tools.AlgBuilder;
import org.polypheny.db.transaction.PolyXid;
import org.polypheny.db.util.Pair;


@Slf4j
class CoWHashIndex extends Index {

    private Map<List<Object>, List<Object>> index = new HashMap<>();
    private boolean initialized = false;

    private Map<PolyXid, Map<List<Object>, List<Object>>> cowIndex = new HashMap<>();
    private Map<PolyXid, List<DeferredIndexUpdate>> cowOpLog = new HashMap<>();
    private Map<PolyXid, List<Pair<List<Object>, List<Object>>>> barrierIndex = new HashMap<>();


    public CoWHashIndex(
            final long id,
            final String name,
            final CatalogSchema schema,
            final CatalogTable table,
            final List<String> columns,
            final List<String> targetColumns ) {
        this.id = id;
        this.name = name;
        this.schema = schema;
        this.table = table;
        this.columns = ImmutableList.copyOf( columns );
        this.targetColumns = ImmutableList.copyOf( targetColumns );
    }


    public CoWHashIndex(
            final long id,
            final String name,
            final CatalogSchema schema,
            final CatalogTable table,
            final String[] columns,
            final String[] targetColumns ) {
        this( id, name, schema, table, Arrays.asList( columns ), Arrays.asList( targetColumns ) );
    }


    @Override
    public String getMethod() {
        return "hash";
    }


    @Override
    public boolean isUnique() {
        return true;
    }


    @Override
    public boolean isPersistent() {
        return false;
    }


    @Override
    void commit( PolyXid xid ) {
        begin( xid );
        if ( barrierIndex.get( xid ).size() > 0 ) {
            throw new IllegalStateException( "Attempted index commit without invoking barrier first" );
        }
        for ( final DeferredIndexUpdate update : this.cowOpLog.get( xid ) ) {
            update.execute( this );
        }
        rollback( xid );
    }


    @Override
    public void barrier( PolyXid xid ) {
        begin( xid );
        for ( final Pair<List<Object>, List<Object>> tuple : barrierIndex.get( xid ) ) {
            postBarrier( xid, tuple.left, tuple.right );
        }
        barrierIndex.get( xid ).clear();
    }


    @Override
    void rollback( PolyXid xid ) {
        this.cowIndex.remove( xid );
        this.cowOpLog.remove( xid );
        this.barrierIndex.remove( xid );
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
        Map<List<Object>, List<Object>> idx;
        if ( (idx = cowIndex.get( xid )) != null ) {
            if ( idx.containsKey( value ) ) {
                return idx.get( value ) != null;
            }
        }
        return index.get( value ) != null;
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
        final Map<List<Object>, List<Object>> ci = cowIndex.get( xid );
        final RexBuilder rexBuilder = builder.getRexBuilder();
        final List<ImmutableList<RexLiteral>> tuples = new ArrayList<>( index.size() + (ci != null ? ci.size() : 0) );
        for ( List<Object> tuple : index.keySet() ) {
            if ( ci != null && ci.containsKey( tuple ) && ci.get( tuple ) == null ) {
                // Tuple was deleted in CoW index
                continue;
            }
            tuples.add( makeRexRow( rowType, rexBuilder, tuple ) );
        }
        if ( ci != null ) {
            for ( Map.Entry<List<Object>, List<Object>> tuple : ci.entrySet() ) {
                if ( tuple.getValue() != null ) {
                    // Tuple was added in CoW index
                    tuples.add( makeRexRow( rowType, rexBuilder, tuple.getKey() ) );
                }
            }
        }

        return (Values) builder.values( ImmutableList.copyOf( tuples ), rowType ).build();
    }


    @Override
    public Values getAsValues( PolyXid xid, AlgBuilder builder, AlgDataType rowType, List<Object> key ) {
        log.error( "{}", index.values().stream().findFirst().get().stream().map( Object::getClass ).collect( Collectors.toList() ) );
        log.error( "{}", key.stream().map( Object::getClass ).collect( Collectors.toList() ) );
        final Map<List<Object>, List<Object>> ci = cowIndex.get( xid );
        final RexBuilder rexBuilder = builder.getRexBuilder();
        List<Object> raw = index.get( key );
        if ( ci != null && ci.containsKey( key ) ) {
            raw = ci.get( key );
        }
        if ( raw == null ) {
            return (Values) builder.values( ImmutableList.of(), rowType ).build();
        }
        return (Values) builder.values( ImmutableList.of( makeRexRow( rowType, rexBuilder, key ) ), rowType ).build();
    }


    @Override
    Map<List<Object>, List<Object>> getRaw() {
        return index;
    }


    @Override
    protected void clear() {
        index.clear();
        cowIndex.clear();
        cowOpLog.clear();
        barrierIndex.clear();
        initialized = false;
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
        List<Pair<List<Object>, List<Object>>> idx = barrierIndex.get( xid );
        idx.add( new Pair<>( key, primary ) );
    }


    protected void postBarrier( PolyXid xid, List<Object> key, List<Object> primary ) {
        Map<List<Object>, List<Object>> idx = cowIndex.get( xid );

        if ( primary == null ) {
            // null = delete
            idx.put( key, null );
            return;
        }
        if ( (idx.containsKey( key ) && idx.get( key ) != null) || index.containsKey( key ) ) {
            throw new ConstraintViolationException(
                    String.format( "Attempt to add duplicate key [%s] to unique index %s", key, name )
            );
        }
        idx.put( key, primary );
    }


    @Override
    void insert( List<Object> key, List<Object> primary ) {
        index.put( key, primary );
    }


    @Override
    public void delete( PolyXid xid, List<Object> key ) {
        begin( xid );
        List<DeferredIndexUpdate> log = cowOpLog.get( xid );

        _delete( xid, key );
        log.add( DeferredIndexUpdate.createDelete( Collections.singleton( key ) ) );
    }


    @Override
    public void deletePrimary( PolyXid xid, List<Object> key, List<Object> primary ) {
        begin( xid );
        List<DeferredIndexUpdate> log = cowOpLog.get( xid );

        _delete( xid, key );
        log.add( DeferredIndexUpdate.createDelete( Collections.singleton( key ) ) );
    }


    @Override
    public void deleteAllPrimary( PolyXid xid, final Iterable<Pair<List<Object>, List<Object>>> values ) {
        begin( xid );
        List<DeferredIndexUpdate> log = cowOpLog.get( xid );

        for ( final Pair<List<Object>, List<Object>> value : values ) {
            _delete( xid, value.left );
        }
        log.add( DeferredIndexUpdate.createDeletePrimary( values ) );
    }


    @Override
    public void deleteAll( PolyXid xid, final Iterable<List<Object>> values ) {
        begin( xid );
        List<DeferredIndexUpdate> log = cowOpLog.get( xid );

        for ( final List<Object> value : values ) {
            _delete( xid, value );
        }
        log.add( DeferredIndexUpdate.createDelete( values ) );
    }


    protected void _delete( PolyXid xid, List<Object> key ) {
        List<Pair<List<Object>, List<Object>>> idx = barrierIndex.get( xid );
        idx.add( new Pair<>( key, null ) );
    }


    @Override
    void delete( List<Object> key ) {
        this.index.remove( key );
    }


    @Override
    void deletePrimary( List<Object> key, List<Object> primary ) {
        this.index.remove( key );
    }


    static class Factory implements IndexFactory {

        @Override
        public boolean canProvide( String method, Boolean unique, Boolean persistent ) {
            return
                    (method == null || method.equals( "hash" ))
                            && (unique == null || unique)
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
            return new CoWHashIndex( id, name, schema, table, columns, targetColumns );
        }

    }

}
