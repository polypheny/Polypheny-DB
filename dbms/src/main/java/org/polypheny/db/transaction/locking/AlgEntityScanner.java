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
public class AlgEntityScanner extends AlgVisitor {

    Map<Lockable, Lockable.LockType> result;


    public AlgEntityScanner() {
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
        result.put( LockablesRegistry.INSTANCE.getOrCreateLockable(unwrapToLockableObject(currentNode.getEntity())), lockType );
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
                .forEach( entry -> result.putIfAbsent( LockablesRegistry.INSTANCE.getOrCreateLockable(entry), LockType.SHARED ) );
    }


    private void visitNonRelationalNode( AlgNode currentNode ) {
        LockType lockType = currentNode.isDataModifying() ? LockType.EXCLUSIVE : LockType.SHARED;
        result.put( LockablesRegistry.INSTANCE.getOrCreateLockable(unwrapToLockableObject(currentNode.getEntity())) , lockType );
    }

    private LockableObject unwrapToLockableObject( Entity entity) {
        Optional<LockableObject> lockableObject = entity.unwrap( LockableObject.class );
        if ( lockableObject.isPresent() ) {
            return lockableObject.get();
        }
        throw new RuntimeException( "Could not unwrap lockableObject" );
    }

}
