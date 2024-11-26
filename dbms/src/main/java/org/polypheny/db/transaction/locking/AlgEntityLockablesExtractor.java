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

package org.polypheny.db.transaction.locking;

import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import lombok.Getter;
import org.polypheny.db.algebra.AlgNode;
import org.polypheny.db.algebra.AlgVisitor;
import org.polypheny.db.algebra.core.relational.RelAlg;
import org.polypheny.db.catalog.Catalog;
import org.polypheny.db.catalog.entity.Entity;
import org.polypheny.db.catalog.entity.logical.LogicalTable;
import org.polypheny.db.config.RuntimeConfig;
import org.polypheny.db.partition.properties.PartitionProperty;
import org.polypheny.db.transaction.locking.Lockable.LockType;

@Getter
public class AlgEntityLockablesExtractor extends AlgVisitor {

    Map<Lockable, Lockable.LockType> result;


    public AlgEntityLockablesExtractor() {
        this.result = new HashMap<>();
    }


    @Override
    public void visit( AlgNode currentNode, int ordinal, AlgNode parentNode ) {
        super.visit( currentNode, ordinal, parentNode );
        if ( currentNode.getEntity() == null ) {
            return;
        }
        if ( currentNode instanceof RelAlg ) {
            visitRelationalNode( currentNode );
            return;
        }
        visitNonRelationalNode( currentNode );
    }


    private void visitRelationalNode( AlgNode currentNode ) {
        LockType lockType = currentNode.isDataModifying() ? LockType.EXCLUSIVE : LockType.SHARED;
        if ( RuntimeConfig.FOREIGN_KEY_ENFORCEMENT.getBoolean() ) {
            extractWriteConstraints( currentNode.getEntity().unwrap( LogicalTable.class ).orElseThrow() );
        }
        addResult( currentNode.getEntity(), lockType);
    }


    private void extractWriteConstraints( LogicalTable logicalTable ) {
        logicalTable.getConstraintIds().stream()
                .flatMap( constraintTableId -> {
                    PartitionProperty property = Catalog.snapshot().alloc().getPartitionProperty( logicalTable.id ).orElseThrow();
                    return property.partitionIds.stream()
                            .map( constraintPartitionId -> Catalog.snapshot().rel().getTable( constraintPartitionId ) )
                            .filter( Optional::isPresent )
                            .map( Optional::get );
                } )
                .forEach( entry -> addResult( entry, LockType.SHARED ) );
    }


    private void visitNonRelationalNode( AlgNode currentNode ) {
        LockType lockType = currentNode.isDataModifying() ? LockType.EXCLUSIVE : LockType.SHARED;
        result.put( LockablesRegistry.INSTANCE.getOrCreateLockable(LockableUtils.unwrapToLockableObject(currentNode.getEntity())) , lockType );
    }

    private void addResult(Entity entity, LockType lockType) {
        switch ((S2plLockingLevel) RuntimeConfig.S2PL_LOCKING_LEVEL.getEnum()) {
            case GLOBAL -> {
                LockType currentLockType = result.get( LockablesRegistry.GLOBAL_SCHEMA_LOCKABLE );
                if ( currentLockType == null || currentLockType == LockType.EXCLUSIVE ) {
                    result.put( LockablesRegistry.GLOBAL_SCHEMA_LOCKABLE, lockType );

                }
            }
            case NAMESPACE -> {
                Lockable lockable = LockablesRegistry.INSTANCE.getOrCreateLockable(LockableUtils.getNamespaceLockableObjectOfEntity(entity));
                LockType currentLockType = result.get( lockable );
                if ( currentLockType == null || currentLockType == LockType.EXCLUSIVE ) {
                    result.put( lockable, lockType );
                }
            }
            case ENTITY -> {
                Lockable lockable = LockablesRegistry.INSTANCE.getOrCreateLockable(entity);
                result.put(lockable, lockType);
            }
        }
    }

}
