/*
 * Copyright 2019-2021 The Polypheny Project
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

package org.polypheny.db.transaction;


import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.stream.Collectors;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.EqualsAndHashCode;
import lombok.NonNull;
import org.jetbrains.annotations.NotNull;
import org.polypheny.db.algebra.AlgNode;
import org.polypheny.db.algebra.AlgVisitor;
import org.polypheny.db.algebra.core.TableModify;
import org.polypheny.db.config.RuntimeConfig;
import org.polypheny.db.plan.AlgOptTable;
import org.polypheny.db.plan.AlgOptUtil;
import org.polypheny.db.prepare.AlgOptTableImpl;
import org.polypheny.db.schema.LogicalTable;
import org.polypheny.db.transaction.Lock.LockMode;


/**
 * <code>TableAccessMap</code> represents the tables accessed by a query plan, with READ/WRITE information.
 */
public class TableAccessMap {


    /**
     * Access mode.
     */
    public enum Mode {
        /**
         * Table is not accessed at all.
         */
        NO_ACCESS,

        /**
         * Table is accessed for read only.
         */
        READ_ACCESS,

        /**
         * Table is accessed for write only.
         */
        WRITE_ACCESS,

        /**
         * Table is accessed for both read and write.
         */
        READWRITE_ACCESS
    }


    private final Map<TableIdentifier, Mode> accessMap;
    private final Map<TableIdentifier, LockMode> accessLockMap;


    /**
     * Constructs a permanently empty TableAccessMap.
     */
    public TableAccessMap() {
        accessMap = Collections.emptyMap();
        accessLockMap = Collections.emptyMap();
    }


    /**
     * Constructs a TableAccessMap for all tables accessed by a {@link AlgNode} and its descendants.
     *
     * @param alg the {@link AlgNode} for which to build the map
     */
    public TableAccessMap( AlgNode alg ) {
        // NOTE: This method must NOT retain a reference to the input alg, because we use it for cached statements, and we
        // don't want to retain any alg references after preparation completes.
        accessMap = new HashMap<>();
        AlgOptUtil.go( new TableRelVisitor(), alg );
        accessLockMap = getAccessLockMap();
    }


    @NotNull
    private Map<TableIdentifier, LockMode> getAccessLockMap() {
        return accessMap.entrySet()
                .stream()
                .filter( e -> Arrays.asList( Mode.READ_ACCESS, Mode.WRITE_ACCESS, Mode.READWRITE_ACCESS ).contains( e.getValue() ) )
                .collect( Collectors.toMap( Entry::getKey, e -> {
                    if ( e.getValue() == Mode.READ_ACCESS ) {
                        return LockMode.SHARED;
                    } else if ( e.getValue() == Mode.WRITE_ACCESS || e.getValue() == Mode.READWRITE_ACCESS ) {
                        return LockMode.EXCLUSIVE;
                    } else {
                        throw new RuntimeException( "LockMode not possible." );
                    }
                } ) );
    }


    /**
     * Constructs a TableAccessMap for a single table
     *
     * @param tableIdentifier fully qualified name of the table, represented as a list
     * @param mode access mode for the table
     */
    public TableAccessMap( TableIdentifier tableIdentifier, Mode mode ) {
        accessMap = new HashMap<>();
        accessMap.put( tableIdentifier, mode );
        accessLockMap = getAccessLockMap();
    }


    /**
     * @return set of qualified names for all tables accessed
     */
    public Set<TableIdentifier> getTablesAccessed() {
        return accessMap.keySet();
    }


    public Collection<Entry<TableIdentifier, LockMode>> getTablesAccessedPair() {
        return accessLockMap.entrySet();
    }


    /**
     * Determines whether a table is accessed at all.
     *
     * @param tableIdentifier qualified name of the table of interest
     * @return true if table is accessed
     */
    public boolean isTableAccessed( TableIdentifier tableIdentifier ) {
        return accessMap.containsKey( tableIdentifier );
    }


    /**
     * Determines whether a table is accessed for read.
     *
     * @param tableIdentifier qualified name of the table of interest
     * @return true if table is accessed for read
     */
    public boolean isTableAccessedForRead( TableIdentifier tableIdentifier ) {
        Mode mode = getTableAccessMode( tableIdentifier );
        return (mode == Mode.READ_ACCESS) || (mode == Mode.READWRITE_ACCESS);
    }


    /**
     * Determines whether a table is accessed for write.
     *
     * @param tableIdentifier qualified name of the table of interest
     * @return true if table is accessed for write
     */
    public boolean isTableAccessedForWrite( TableIdentifier tableIdentifier ) {
        Mode mode = getTableAccessMode( tableIdentifier );
        return (mode == Mode.WRITE_ACCESS) || (mode == Mode.READWRITE_ACCESS);
    }


    /**
     * Determines the access mode of a table.
     *
     * @param tableIdentifier qualified name of the table of interest
     * @return access mode
     */
    public Mode getTableAccessMode( @NonNull TableIdentifier tableIdentifier ) {
        Mode mode = accessMap.get( tableIdentifier );
        if ( mode == null ) {
            return Mode.NO_ACCESS;
        }
        return mode;
    }


    /**
     * Constructs a qualified name for an optimizer table reference.
     *
     * @param table table of interest
     * @return qualified name
     */
    public TableIdentifier getQualifiedName( AlgOptTable table ) {
        if ( !(table instanceof AlgOptTableImpl) ) {
            throw new RuntimeException( "Unexpected table type: " + table.getClass() );
        }
        if ( !(((AlgOptTableImpl) table).getTable() instanceof LogicalTable) ) {
            throw new RuntimeException( "Unexpected table type: " + ((AlgOptTableImpl) table).getTable().getClass() );
        }
        return new TableIdentifier( ((LogicalTable) ((AlgOptTableImpl) table).getTable()).getTableId() );
    }


    /**
     * Visitor that finds all tables in a tree.
     */
    private class TableRelVisitor extends AlgVisitor {

        @Override
        public void visit( AlgNode p, int ordinal, AlgNode parent ) {
            super.visit( p, ordinal, parent );
            AlgOptTable table = p.getTable();
            if ( table == null ) {
                return;
            }
            Mode newAccess;

            // FIXME: Don't rely on object type here; eventually someone is going to write a rule which transforms to
            //  something which doesn't inherit TableModify, and this will break. Need to make this explicit in the
            //  {@link AlgNode} interface.
            if ( p instanceof TableModify ) {
                newAccess = Mode.WRITE_ACCESS;
                if ( RuntimeConfig.FOREIGN_KEY_ENFORCEMENT.getBoolean() ) {
                    LogicalTable logicalTable = (LogicalTable) table.getTable();
                    for ( Long constraintTable : logicalTable.getConstraintIds() ) {
                        TableIdentifier id = new TableIdentifier( constraintTable );
                        if ( !accessMap.containsKey( id ) ) {
                            accessMap.put( id, Mode.READ_ACCESS );
                        }
                    }
                }

            } else {
                newAccess = Mode.READ_ACCESS;
            }
            TableIdentifier key = getQualifiedName( table );
            Mode oldAccess = accessMap.get( key );
            if ( (oldAccess != null) && (oldAccess != newAccess) ) {
                newAccess = Mode.READWRITE_ACCESS;
            }
            accessMap.put( key, newAccess );
        }

    }


    @Data
    @AllArgsConstructor
    @EqualsAndHashCode
    public static class TableIdentifier {

        long tableId;

    }

}
