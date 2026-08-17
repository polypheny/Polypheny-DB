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
import lombok.extern.slf4j.Slf4j;
import org.polypheny.db.algebra.core.Values;
import org.polypheny.db.algebra.exceptions.ConstraintViolationException;
import org.polypheny.db.algebra.type.AlgDataType;
import org.polypheny.db.catalog.entity.logical.LogicalNamespace;
import org.polypheny.db.catalog.entity.logical.LogicalTable;
import org.polypheny.db.rex.RexBuilder;
import org.polypheny.db.rex.RexLiteral;
import org.polypheny.db.tools.AlgBuilder;
import org.polypheny.db.transaction.PolyXid;
import org.polypheny.db.type.entity.PolyValue;
import org.polypheny.db.util.Pair;

@Slf4j
class CoWHashIndex extends Index {

    private final Map<List<PolyValue>, List<PolyValue>> index = new HashMap<>();
    private boolean initialized = false;

    private final Map<PolyXid, Map<List<PolyValue>, List<PolyValue>>> cowIndex = new HashMap<>();
    private final Map<PolyXid, List<DeferredIndexUpdate>> cowOpLog = new HashMap<>();
    private final Map<PolyXid, List<Pair<List<PolyValue>, List<PolyValue>>>> barrierIndex = new HashMap<>();
    
    // NEW: Track read-only transactions to optimize memory usage
    private final Set<PolyXid> readOnlyTransactions = new HashSet<>();

    public CoWHashIndex(
            final long id,
            final String name,
            final LogicalNamespace schema,
            final LogicalTable table,
            final List<String> columns,
            final List<String> targetColumns ) {
        this.id = id;
        this.name = name;
        this.schema = schema;
        this.table = table;
        this.columns = ImmutableList.copyOf( columns );
        this.targetColumns = ImmutableList.copyOf( targetColumns );
        
        // NEW: Log index creation for debugging
        if (log.isDebugEnabled()) {
            log.debug("Created CoWHashIndex {} for table {}.{} with columns {}", 
                name, schema.getName(), table.getName(), columns);
        }
    }

    public CoWHashIndex(
            final long id,
            final String name,
            final LogicalNamespace schema,
            final LogicalTable table,
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
        if ( !barrierIndex.get( xid ).isEmpty() ) {
            throw new IllegalStateException( "Attempted index commit without invoking barrier first" );
        }
        
        // NEW: Performance logging
        long startTime = System.nanoTime();
        int operationCount = cowOpLog.get( xid ).size();
        
        for ( final DeferredIndexUpdate update : this.cowOpLog.get( xid ) ) {
            update.execute( this );
        }
        
        // NEW: Log performance metrics
        if (log.isDebugEnabled()) {
            long duration = System.nanoTime() - startTime;
            log.debug("Committed {} operations for transaction {} in {} ns", 
                operationCount, xid, duration);
        }
        
        rollback( xid );
    }

    @Override
    public void barrier( PolyXid xid ) {
        begin( xid );
        
        // NEW: Batch validation for better performance
        validateBatchOperations( xid );
        
        for ( final Pair<List<PolyValue>, List<PolyValue>> tuple : barrierIndex.get( xid ) ) {
            postBarrier( xid, tuple.left, tuple.right );
        }
        barrierIndex.get( xid ).clear();
    }

    @Override
    void rollback( PolyXid xid ) {
        this.cowIndex.remove( xid );
        this.cowOpLog.remove( xid );
        this.barrierIndex.remove( xid );
        // NEW: Clean up read-only transaction tracking
        this.readOnlyTransactions.remove( xid );
    }

    // NEW: Enhanced begin method with lazy initialization
    protected void begin( PolyXid xid ) {
        if ( !cowIndex.containsKey( xid ) ) {
            IndexManager.getInstance().begin( xid, this );
            // Mark as read-only transaction initially
            readOnlyTransactions.add( xid );
        }
    }
    
    // NEW: Initialize write structures only when needed
    protected void beginWrite( PolyXid xid ) {
        begin( xid );
        if ( readOnlyTransactions.contains( xid ) ) {
            // Convert from read-only to write transaction
            cowIndex.put( xid, new HashMap<>() );
            cowOpLog.put( xid, new ArrayList<>() );
            barrierIndex.put( xid, new ArrayList<>() );
            readOnlyTransactions.remove( xid );
            
            if (log.isTraceEnabled()) {
                log.trace("Converted transaction {} from read-only to write", xid);
            }
        }
    }

    // ENHANCED: Optimized contains method with single lookup
    @Override
    public boolean contains( PolyXid xid, List<PolyValue> value ) {
        // NEW: Fast path for read-only transactions
        if ( readOnlyTransactions.contains( xid ) ) {
            return index.get( value ) != null;
        }
        
        Map<List<PolyValue>, List<PolyValue>> idx = cowIndex.get( xid );
        if ( idx != null ) {
            // FIXED: Single lookup instead of containsKey + get
            List<PolyValue> cowValue = idx.get( value );
            if ( cowValue != null ) {
                return true; // Found in CoW index and not deleted
            }
            if ( idx.containsKey( value ) ) {
                return false; // Found in CoW index but marked as deleted (null value)
            }
        }
        return index.get( value ) != null;
    }

    // ENHANCED: Early exit optimization
    @Override
    public boolean containsAny( PolyXid xid, Iterable<List<PolyValue>> values ) {
        for ( final List<PolyValue> tuple : values ) {
            if ( contains( xid, tuple ) ) {
                return true; // Early exit on first match
            }
        }
        return false;
    }

    // ENHANCED: Early exit optimization
    @Override
    public boolean containsAll( PolyXid xid, Iterable<List<PolyValue>> values ) {
        for ( final List<PolyValue> tuple : values ) {
            if ( !contains( xid, tuple ) ) {
                return false; // Early exit on first non-match
            }
        }
        return true;
    }

    // ENHANCED: Optimized getAsValues with better capacity estimation
    @Override
    public Values getAsValues( PolyXid xid, AlgBuilder builder, AlgDataType rowType ) {
        final Map<List<PolyValue>, List<PolyValue>> ci = cowIndex.get( xid );
        final RexBuilder rexBuilder = builder.getRexBuilder();
        
        // NEW: Better capacity estimation
        int estimatedSize = index.size();
        if ( ci != null ) {
            estimatedSize += ci.size();
        }
        final List<ImmutableList<RexLiteral>> tuples = new ArrayList<>( estimatedSize );
        
        for ( List<PolyValue> tuple : index.keySet() ) {
            if ( ci != null && ci.containsKey( tuple ) && ci.get( tuple ) == null ) {
                // Tuple was deleted in CoW index
                continue;
            }
            tuples.add( makeRexRow( rowType, rexBuilder, tuple ) );
        }
        if ( ci != null ) {
            for ( Map.Entry<List<PolyValue>, List<PolyValue>> tuple : ci.entrySet() ) {
                if ( tuple.getValue() != null ) {
                    // Tuple was added in CoW index
                    tuples.add( makeRexRow( rowType, rexBuilder, tuple.getKey() ) );
                }
            }
        }

        return (Values) builder.values( ImmutableList.copyOf( tuples ), rowType ).build();
    }

    // ENHANCED: Single lookup optimization
    @Override
    public Values getAsValues( PolyXid xid, AlgBuilder builder, AlgDataType rowType, List<PolyValue> key ) {
        final Map<List<PolyValue>, List<PolyValue>> ci = cowIndex.get( xid );
        final RexBuilder rexBuilder = builder.getRexBuilder();
        List<PolyValue> raw = index.get( key );
        
        // FIXED: Avoid redundant lookup
        if ( ci != null ) {
            List<PolyValue> cowValue = ci.get( key );
            if ( cowValue != null || ci.containsKey( key ) ) {
                raw = cowValue; // Use CoW value (might be null for deleted)
            }
        }
        
        if ( raw == null ) {
            return (Values) builder.values( ImmutableList.of(), rowType ).build();
        }
        return (Values) builder.values( ImmutableList.of( makeRexRow( rowType, rexBuilder, key ) ), rowType ).build();
    }

    @Override
    Map<?, ?> getRaw() {
        return index;
    }

    @Override
    protected void clear() {
        index.clear();
        cowIndex.clear();
        cowOpLog.clear();
        barrierIndex.clear();
        readOnlyTransactions.clear(); // NEW: Clear read-only tracking
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

    // ENHANCED: Optimized batch insert with pre-validation
    // ENHANCED: Optimized batch insert with pre-validation
    @Override
    public void insertAll( PolyXid xid, final Iterable<Pair<List<PolyValue>, List<PolyValue>>> values ) {
        beginWrite( xid ); // NEW: Use lazy write initialization
        
        // NEW: Pre-validate batch for constraint violations
        validateBatchUniqueness( xid, values );
        
        List<DeferredIndexUpdate> log = cowOpLog.get( xid );
        
        // NEW: Pre-allocate capacity if possible - FIXED
        if ( values instanceof List ) {
            List<Pair<List<PolyValue>, List<PolyValue>>> valuesList = 
                (List<Pair<List<PolyValue>, List<PolyValue>>>) values;
            List<Pair<List<PolyValue>, List<PolyValue>>> barrierList = barrierIndex.get( xid );
            
            // Only use ensureCapacity if it's an ArrayList
            if ( barrierList instanceof ArrayList ) {
                ((ArrayList<Pair<List<PolyValue>, List<PolyValue>>>) barrierList)
                    .ensureCapacity( barrierList.size() + valuesList.size() );
            }
        }
        
        for ( final Pair<List<PolyValue>, List<PolyValue>> row : values ) {
            _insert( xid, row.getKey(), row.getValue() );
        }
        log.add( DeferredIndexUpdate.createInsert( values ) );
    }

    @Override
    public void insert( PolyXid xid, List<PolyValue> key, List<PolyValue> primary ) {
        beginWrite( xid ); // NEW: Use lazy write initialization
        List<DeferredIndexUpdate> log = cowOpLog.get( xid );
        _insert( xid, key, primary );
        log.add( DeferredIndexUpdate.createInsert( Collections.singleton( new Pair<>( key, primary ) ) ) );
    }

    protected void _insert( PolyXid xid, List<PolyValue> key, List<PolyValue> primary ) {
        List<Pair<List<PolyValue>, List<PolyValue>>> idx = barrierIndex.get( xid );
        idx.add( new Pair<>( key, primary ) );
    }

    // ENHANCED: Better error messages and single lookup
    protected void postBarrier( PolyXid xid, List<PolyValue> key, List<PolyValue> primary ) {
        Map<List<PolyValue>, List<PolyValue>> idx = cowIndex.get( xid );

        if ( primary == null ) {
            // null = delete
            idx.put( key, null );
            return;
        }
        
        // ENHANCED: Single lookup for CoW index check
        List<PolyValue> existingCowValue = idx.get( key );
        boolean existsInCow = existingCowValue != null || idx.containsKey( key );
        
        if ( (existsInCow && existingCowValue != null) || index.containsKey( key ) ) {
            // NEW: Enhanced error message with more context
            throw new ConstraintViolationException(
                    String.format( "Attempt to add duplicate key %s to unique index %s on table %s.%s", 
                        key, name, schema.getName(), table.getName() )
            );
        }
        idx.put( key, primary );
    }

    @Override
    void insert( List<PolyValue> key, List<PolyValue> primary ) {
        index.put( key, primary );
    }

    @Override
    public void delete( PolyXid xid, List<PolyValue> key ) {
        beginWrite( xid ); // NEW: Use lazy write initialization
        List<DeferredIndexUpdate> log = cowOpLog.get( xid );

        _delete( xid, key );
        log.add( DeferredIndexUpdate.createDelete( Collections.singleton( key ) ) );
    }

    @Override
    public void deletePrimary( PolyXid xid, List<PolyValue> key, List<PolyValue> primary ) {
        beginWrite( xid ); // NEW: Use lazy write initialization
        List<DeferredIndexUpdate> log = cowOpLog.get( xid );

        _delete( xid, key );
        log.add( DeferredIndexUpdate.createDelete( Collections.singleton( key ) ) );
    }

    // ENHANCED: Optimized batch delete
    @Override
    public void deleteAllPrimary( PolyXid xid, final Iterable<Pair<List<PolyValue>, List<PolyValue>>> values ) {
        beginWrite( xid ); // NEW: Use lazy write initialization
        List<DeferredIndexUpdate> log = cowOpLog.get( xid );

        // NEW: Pre-allocate capacity if possible
        if ( values instanceof List ) {
            List<Pair<List<PolyValue>, List<PolyValue>>> valuesList = 
                (List<Pair<List<PolyValue>, List<PolyValue>>>) values;
            List<Pair<List<PolyValue>, List<PolyValue>>> barrierList = barrierIndex.get( xid );
            
            // Only use ensureCapacity if it's an ArrayList
            if ( barrierList instanceof ArrayList ) {
                ((ArrayList<Pair<List<PolyValue>, List<PolyValue>>>) barrierList)
                    .ensureCapacity( barrierList.size() + valuesList.size() );
            }
        }

        for ( final Pair<List<PolyValue>, List<PolyValue>> value : values ) {
            _delete( xid, value.left );
        }
        log.add( DeferredIndexUpdate.createDeletePrimary( values ) );
    }


        @Override
    public void deleteAll( PolyXid xid, final Iterable<List<PolyValue>> values ) {
        beginWrite( xid ); // NEW: Use lazy write initialization
        List<DeferredIndexUpdate> log = cowOpLog.get( xid );

        // NEW: Pre-allocate capacity if possible
        if ( values instanceof List ) {
            List<List<PolyValue>> valuesList = (List<List<PolyValue>>) values;
            List<Pair<List<PolyValue>, List<PolyValue>>> barrierList = barrierIndex.get( xid );
            
            // Only use ensureCapacity if it's an ArrayList
            if ( barrierList instanceof ArrayList ) {
                ((ArrayList<Pair<List<PolyValue>, List<PolyValue>>>) barrierList)
                    .ensureCapacity( barrierList.size() + valuesList.size() );
            }
        }

        for ( final List<PolyValue> value : values ) {
            _delete( xid, value );
        }
        log.add( DeferredIndexUpdate.createDelete( values ) );
    }


    protected void _delete( PolyXid xid, List<PolyValue> key ) {
        List<Pair<List<PolyValue>, List<PolyValue>>> idx = barrierIndex.get( xid );
        idx.add( new Pair<>( key, null ) );
    }

    @Override
    void delete( List<PolyValue> key ) {
        this.index.remove( key );
    }

    @Override
    void deletePrimary( List<PolyValue> key, List<PolyValue> primary ) {
        this.index.remove( key );
    }

    // NEW: Batch validation method for uniqueness constraints
    private void validateBatchUniqueness( PolyXid xid, Iterable<Pair<List<PolyValue>, List<PolyValue>>> values ) {
        Set<List<PolyValue>> batchKeys = new HashSet<>();
        for ( Pair<List<PolyValue>, List<PolyValue>> pair : values ) {
            List<PolyValue> key = pair.left;
            
            // Check for duplicates within the batch
            if ( !batchKeys.add( key ) ) {
                throw new ConstraintViolationException(
                    String.format( "Duplicate key %s found within batch for unique index %s", key, name )
                );
            }
            
            // Check for existing keys
            if ( contains( xid, key ) ) {
                throw new ConstraintViolationException(
                    String.format( "Attempt to add duplicate key %s to unique index %s", key, name )
                );
            }
        }
    }

    // NEW: Validate all pending barrier operations
    private void validateBatchOperations( PolyXid xid ) {
        List<Pair<List<PolyValue>, List<PolyValue>>> operations = barrierIndex.get( xid );
        if ( operations == null || operations.isEmpty() ) {
            return;
        }

        Set<List<PolyValue>> seenKeys = new HashSet<>();
        for ( Pair<List<PolyValue>, List<PolyValue>> operation : operations ) {
            List<PolyValue> key = operation.left;
            List<PolyValue> value = operation.right;
            
            if ( value != null ) { // Insert operation
                if ( !seenKeys.add( key ) ) {
                    throw new ConstraintViolationException(
                        String.format( "Multiple insert operations for key %s in transaction %s", key, xid )
                    );
                }
            }
        }
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
                LogicalNamespace schema,
                LogicalTable table,
                List<String> columns,
                List<String> targetColumns ) {
            return new CoWHashIndex( id, name, schema, table, columns, targetColumns );
        }
    }
}
