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

package org.polypheny.db.transaction;


import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.stream.Collectors;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NonNull;
import lombok.extern.slf4j.Slf4j;
import org.jetbrains.annotations.NotNull;
import org.polypheny.db.algebra.AlgNode;
import org.polypheny.db.algebra.AlgVisitor;
import org.polypheny.db.algebra.core.document.DocumentAlg;
import org.polypheny.db.algebra.core.document.DocumentModify;
import org.polypheny.db.algebra.core.document.DocumentScan;
import org.polypheny.db.algebra.core.lpg.LpgAlg;
import org.polypheny.db.algebra.core.lpg.LpgModify;
import org.polypheny.db.algebra.core.lpg.LpgScan;
import org.polypheny.db.algebra.core.relational.RelModify;
import org.polypheny.db.catalog.Catalog;
import org.polypheny.db.catalog.entity.Entity;
import org.polypheny.db.catalog.entity.allocation.AllocationEntity;
import org.polypheny.db.catalog.entity.logical.LogicalTable;
import org.polypheny.db.catalog.exceptions.GenericRuntimeException;
import org.polypheny.db.catalog.logistic.DataModel;
import org.polypheny.db.config.RuntimeConfig;
import org.polypheny.db.partition.properties.PartitionProperty;
import org.polypheny.db.plan.AlgOptUtil;
import org.polypheny.db.transaction.EntityAccessMap.EntityIdentifier.NamespaceLevel;
import org.polypheny.db.transaction.Lock.LockMode;


/**
 * <code>EntityAccessMap</code> represents the entities accessed by a query plan, with READ/WRITE information.
 */
@Slf4j
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
    private final Map<EntityIdentifier, LockMode> accessLockMap;

    private final Map<Long, List<Long>> accessedPartitions;


    /**
     * Constructs a permanently empty EntityAccessMap.
     */
    public EntityAccessMap() {
        accessMap = Collections.emptyMap();
        accessLockMap = Collections.emptyMap();
        accessedPartitions = Collections.emptyMap();
    }


    /**
     * Constructs a EntityAccessMap for all entities accessed by a {@link AlgNode} and its descendants.
     *
     * @param alg the {@link AlgNode} for which to build the map
     * @param accessedPartitions entityId to partitions
     */
    public EntityAccessMap( AlgNode alg, Map<Long, List<Long>> accessedPartitions ) {
        // NOTE: This method must NOT retain a reference to the input alg, because we use it for cached statements, and we
        // don't want to retain any alg references after preparation completes.
        this.accessMap = new HashMap<>();

        //TODO @HENNLO remove this and rather integrate EntityAccessMap directly into Query Processor when DML Partitions can be queried
        this.accessedPartitions = accessedPartitions;

        AlgOptUtil.go( new EntityAlgVisitor(), alg );
        this.accessLockMap = evaluateAccessLockMap();
    }


    /**
     * Constructs a EntityAccessMap for a single entity
     *
     * @param entityIdentifier fully qualified name of the entity, represented as a list
     * @param mode access mode for the entity
     */
    public EntityAccessMap( EntityIdentifier entityIdentifier, Mode mode ) {
        accessMap = new HashMap<>();
        accessMap.put( entityIdentifier, mode );
        accessLockMap = evaluateAccessLockMap();

        this.accessedPartitions = new HashMap<>();
    }


    @NotNull
    private Map<EntityIdentifier, LockMode> evaluateAccessLockMap() {
        return accessMap.entrySet()
                .stream()
                .filter( e -> Arrays.asList( Mode.READ_ACCESS, Mode.WRITE_ACCESS, Mode.READWRITE_ACCESS ).contains( e.getValue() ) )
                .collect( Collectors.toMap( Entry::getKey, e -> {
                    if ( e.getValue() == Mode.READ_ACCESS ) {
                        return LockMode.SHARED;
                    } else if ( e.getValue() == Mode.WRITE_ACCESS || e.getValue() == Mode.READWRITE_ACCESS ) {
                        return LockMode.EXCLUSIVE;
                    } else {
                        throw new GenericRuntimeException( "LockMode not possible." );
                    }
                } ) );
    }


    /**
     * @return set of qualified names for all accessed entities
     */
    public Set<EntityIdentifier> getAccessedEntities() {
        return accessMap.keySet();
    }


    /**
     * Return the required lock mode per entity analogously to the entity access mode.
     *
     * @return all accessed entities and their lock mode
     */
    public Collection<Entry<EntityIdentifier, LockMode>> getAccessedEntityPair() {
        return accessLockMap.entrySet();
    }


    /**
     * Determines whether an entity is accessed at all.
     *
     * @param entityIdentifier qualified name of the entity of interest
     * @return true if entity is accessed
     */
    public boolean isEntityAccessed( EntityIdentifier entityIdentifier ) {
        return accessMap.containsKey( entityIdentifier );
    }


    /**
     * Determines whether an entity is accessed for read.
     *
     * @param entityIdentifier qualified name of the entity of interest
     * @return true if entity is accessed for read
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
     * Determines the access mode of an entity.
     *
     * @param entityIdentifier qualified name of the entity of interest
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
     * Constructs a qualified name for an optimizer entity reference.
     *
     * @param entity entity of interest
     * @return qualified name
     */
    public EntityIdentifier getQualifiedName( Entity entity, long partitionId ) {
        return new EntityIdentifier( entity.id, partitionId, NamespaceLevel.ENTITY_LEVEL );
    }


    /**
     * Visitor that finds all entities in a tree.
     */
    private class EntityAlgVisitor extends AlgVisitor {


        @Override
        public void visit( AlgNode p, int ordinal, AlgNode parent ) {
            super.visit( p, ordinal, parent );
            Entity entity = p.getEntity();
            if ( entity == null ) {
                return;
            }

            if ( p instanceof LpgAlg ) {
                attachLpg( p );
                return;
            }
            if ( p instanceof DocumentAlg ) {
                attachDoc( (AlgNode & DocumentAlg) p );
                return;
            }

            attachRel( p, entity );
        }


        private void attachRel( AlgNode p, Entity entity ) {
            Mode newAccess;

            //  FIXME: Don't rely on object type here; eventually someone is going to write a rule which transforms to
            //  something which doesn't inherit Modify, and this will break. Need to make this explicit in the
            //  {@link AlgNode} interface.
            if ( p.unwrap( RelModify.class ).isPresent() ) {
                newAccess = Mode.WRITE_ACCESS;
                if ( RuntimeConfig.FOREIGN_KEY_ENFORCEMENT.getBoolean() ) {
                    extractWriteConstraints( entity.unwrap( LogicalTable.class ).orElseThrow() );
                }
            } else {
                newAccess = Mode.READ_ACCESS;
            }

            // TODO @HENNLO Integrate PartitionIds into Entities
            // If entity has no info which partitions are accessed, ergo has no concrete entries in map
            // assume that all are accessed. --> Add all to AccessMap
            List<Long> relevantAllocations;
            if ( accessedPartitions.containsKey( p.getEntity().id ) ) {
                relevantAllocations = accessedPartitions.get( p.getEntity().id );
            } else {
                if ( entity.dataModel == DataModel.RELATIONAL ) {
                    List<AllocationEntity> allocations = Catalog.getInstance().getSnapshot().alloc().getFromLogical( entity.id );
                    relevantAllocations = allocations.stream().map( a -> a.id ).toList();
                } else {
                    relevantAllocations = List.of();
                }

            }

            for ( long allocationId : relevantAllocations ) {
                EntityIdentifier key = getQualifiedName( entity, allocationId );
                Mode oldAccess = accessMap.get( key );
                if ( (oldAccess != null) && (oldAccess != newAccess) ) {
                    newAccess = Mode.READWRITE_ACCESS;
                }
                accessMap.put( key, newAccess );
            }
        }


        private <T extends AlgNode & DocumentAlg> void attachDoc( T p ) {

            Mode newAccess;
            if ( p instanceof DocumentModify ) {
                newAccess = Mode.WRITE_ACCESS;
            } else if ( p instanceof DocumentScan<?> ) {
                newAccess = Mode.READ_ACCESS;
            } else {
                return;
            }
            // as documents are using the same id space as entity this will work
            EntityIdentifier key = new EntityIdentifier( p.getId(), 0, NamespaceLevel.ENTITY_LEVEL );
            accessMap.put( key, newAccess );
        }


        private void attachLpg( AlgNode p ) {

            Mode newAccess;
            if ( p instanceof LpgModify ) {
                newAccess = Mode.WRITE_ACCESS;
            } else if ( p instanceof LpgScan<?> ) {
                newAccess = Mode.READ_ACCESS;
            } else {
                return;
            }
            // as graph is on the namespace level
            EntityIdentifier key = new EntityIdentifier( p.getEntity().id, 0, NamespaceLevel.NAMESPACE_LEVEL );
            accessMap.put( key, newAccess );
        }


        /**
         * Retrieves an access map for linked logical based on foreign key constraints
         */
        private void extractWriteConstraints( LogicalTable logicalTable ) {

            for ( long constraintTable : logicalTable.getConstraintIds() ) {
                PartitionProperty property = Catalog.getInstance().getSnapshot().alloc().getPartitionProperty( logicalTable.id ).orElseThrow();
                for ( long constraintPartitionIds : property.partitionIds ) {

                    EntityIdentifier id = new EntityIdentifier( constraintTable, constraintPartitionIds, NamespaceLevel.ENTITY_LEVEL );
                    if ( !accessMap.containsKey( id ) ) {
                        accessMap.put( id, Mode.READ_ACCESS );
                    }
                }
            }
        }

    }


    @Data
    @AllArgsConstructor
    public static class EntityIdentifier {

        long entityId;

        long allocationId;
        // the locking checks for an existing EntityIdentifier, which is identified, by its id a partition
        // this is done on the entity level, the graph model defines the graph on the namespace level and this could lead to conflicts
        // therefore we can add the level to the identifier
        NamespaceLevel namespaceLevel;


        public enum NamespaceLevel {
            NAMESPACE_LEVEL,
            ENTITY_LEVEL
        }


        @Override
        public boolean equals( Object o ) {
            if ( this == o ) {
                return true;
            }
            if ( o == null || getClass() != o.getClass() ) {
                return false;
            }

            EntityIdentifier that = (EntityIdentifier) o;

            if ( entityId != that.entityId ) {
                return false;
            }
            return allocationId == that.allocationId;
        }


        @Override
        public int hashCode() {
            int result = (int) (entityId ^ (entityId >>> 32));
            result = 31 * result + (int) (allocationId ^ (allocationId >>> 32));
            result = 31 * result + (namespaceLevel != null ? namespaceLevel.hashCode() : 0);
            return result;
        }

    }

}
