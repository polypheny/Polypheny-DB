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


import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.EqualsAndHashCode;
import lombok.NonNull;
import org.polypheny.db.algebra.AlgNode;
import org.polypheny.db.algebra.AlgVisitor;
import org.polypheny.db.algebra.core.TableModify;
import org.polypheny.db.catalog.Catalog;
import org.polypheny.db.plan.AlgOptTable;
import org.polypheny.db.plan.AlgOptUtil;
import org.polypheny.db.prepare.AlgOptTableImpl;
import org.polypheny.db.schema.LogicalTable;


/**
 * <code>EntityAccessMap</code> represents the entities accessed by a query plan, with READ/WRITE information.
 */
public class EntityAccessMap {

    /**
     * Access mode.
     */
    public enum Mode {
        /**
         * Entity is not accessed at all.
         */
        NO_ACCESS,

        /**
         * Entity is accessed for read only.
         */
        READ_ACCESS,

        /**
         * Entity is accessed for write only.
         */
        WRITE_ACCESS,

        /**
         * Entity is accessed for both read and write.
         */
        READWRITE_ACCESS
    }


    private final Map<EntityIdentifier, Mode> accessMap;

    private final Map<Integer, List<Long>> accessedPartitions;


    /**
     * Constructs a permanently empty EntityAccessMap.
     */
    public EntityAccessMap() {
        this.accessMap = Collections.emptyMap();
        this.accessedPartitions = new HashMap<>();
    }


    /**
     * Constructs a EntityAccessMap for all entities accessed by a {@link AlgNode} and its descendants.
     *
     * @param alg the {@link AlgNode} for which to build the map
     * @param accessedPartitions tableScanId to Partitions
     */
    public EntityAccessMap( AlgNode alg, Map<Integer, List<Long>> accessedPartitions ) {
        // NOTE: This method must NOT retain a reference to the input alg, because we use it for cached statements, and we
        // don't want to retain any alg references after preparation completes.
        this.accessMap = new HashMap<>();

        //TODO @HENNLO remove this and rather integrate Entity AccessMap directly into Query Processor when DML Partitions can be queried
        this.accessedPartitions = accessedPartitions;
        AlgOptUtil.go( new TableRelVisitor(), alg );
    }


    /**
     * Constructs a EntityAccessMap for a single table
     *
     * @param entityIdentifier fully qualified name of the entity, represented as a list
     * @param mode access mode for the table
     */
    public EntityAccessMap( EntityIdentifier entityIdentifier, Mode mode ) {
        this.accessMap = new HashMap<>();
        this.accessMap.put( entityIdentifier, mode );

        this.accessedPartitions = new HashMap<>();
    }


    /**
     * @return set of qualified names for all accessed entities
     */
    public Set<EntityIdentifier> getAccessedEntities() {
        return accessMap.keySet();
    }


    /**
     * Determines whether a entity is accessed at all.
     *
     * @param entityIdentifier qualified name of the entity of interest
     * @return true if table is accessed
     */
    public boolean isEntityAccessed( EntityIdentifier entityIdentifier ) {
        return accessMap.containsKey( entityIdentifier );
    }


    /**
     * Determines whether an Entity is accessed for read.
     *
     * @param entityIdentifier qualified name of the Entity of interest
     * @return true if Entity is accessed for read
     */
    public boolean isEntityAccessedForRead( EntityIdentifier entityIdentifier ) {
        Mode mode = getEntityAccessMode( entityIdentifier );
        return (mode == Mode.READ_ACCESS) || (mode == Mode.READWRITE_ACCESS);
    }


    /**
     * Determines whether an Entity is accessed for write.
     *
     * @param entityIdentifier qualified name of the Entity of interest
     * @return true if Entity is accessed for write
     */
    public boolean isEntityAccessedForWrite( EntityIdentifier entityIdentifier ) {
        Mode mode = getEntityAccessMode( entityIdentifier );
        return (mode == Mode.WRITE_ACCESS) || (mode == Mode.READWRITE_ACCESS);
    }


    /**
     * Determines the access mode of a Entity.
     *
     * @param entityIdentifier qualified name of the Entity of interest
     * @return access mode
     */
    public Mode getEntityAccessMode( @NonNull EntityAccessMap.EntityIdentifier entityIdentifier ) {
        Mode mode = accessMap.get( entityIdentifier );
        if ( mode == null ) {
            return Mode.NO_ACCESS;
        }
        return mode;
    }


    /**
     * Constructs a qualified name for an optimizer Entity reference.
     *
     * @param table table of interest
     * @param partitionId
     * @return qualified name
     */
    public EntityIdentifier getQualifiedName( AlgOptTable table, long partitionId ) {
        if ( !(table instanceof AlgOptTableImpl) ) {
            throw new RuntimeException( "Unexpected table type: " + table.getClass() );
        }
        if ( !(((AlgOptTableImpl) table).getTable() instanceof LogicalTable) ) {
            throw new RuntimeException( "Unexpected table type: " + ((AlgOptTableImpl) table).getTable().getClass() );
        }
        return new EntityIdentifier( ((LogicalTable) ((AlgOptTableImpl) table).getTable()).getTableId(), partitionId );
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

            List<Long> relevantPartitions;
            if ( accessedPartitions.containsKey( p.getId() ) ) {
                relevantPartitions = accessedPartitions.get( p.getId() );
            } else {
                relevantPartitions = Catalog.getInstance().getTable( table.getTable().getTableId() ).partitionProperty.partitionIds;
            }

            for ( long partitionId : relevantPartitions ) {
                Mode newAccess;

                // TODO @HENNLO Integrate PartitionIds into Entities
                // If table has no info which partitions are accessed, ergo has no concrete entries in map
                // assume that all are accessed. --> Add all to AccessMap

                // FIXME: Don't rely on object type here; eventually someone is going to write a rule which transforms to
                //  something which doesn't inherit TableModify, and this will break. Need to make this explicit in the
                //  {@link AlgNode} interface.
                if ( p instanceof TableModify ) {
                    newAccess = Mode.WRITE_ACCESS;
                } else {
                    newAccess = Mode.READ_ACCESS;
                }
                EntityIdentifier key = getQualifiedName( table, partitionId );
                Mode oldAccess = accessMap.get( key );
                if ( (oldAccess != null) && (oldAccess != newAccess) ) {
                    newAccess = Mode.READWRITE_ACCESS;
                }
                accessMap.put( key, newAccess );
            }
        }

    }


    @Data
    @AllArgsConstructor
    @EqualsAndHashCode
    public static class EntityIdentifier {

        long tableId;
        long partitionId;

    }

}
