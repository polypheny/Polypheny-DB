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

import java.util.Optional;
import lombok.NonNull;
import org.polypheny.db.catalog.Catalog;
import org.polypheny.db.catalog.entity.Entity;
import org.polypheny.db.catalog.entity.logical.LogicalNamespace;

public class LockableUtils {

    public static LockableObject unwrapToLockableObject( Entity entity ) {
        Optional<LockableObject> lockableObject = entity.unwrap( LockableObject.class );
        if ( lockableObject.isPresent() ) {
            return lockableObject.get();
        }
        throw new RuntimeException( "Could not unwrap lockableObject" );
    }


    public static Lockable convertToLockable( @NonNull LockableObject lockableObject ) {
        switch ( lockableObject.getObjectType() ) {
            case NAMESPACE -> {
                return convertNamespaceToLockable( lockableObject );
            }

            case ENTITY -> {
                return convertEntityToLockable( lockableObject );
            }

            default -> {
                throw new IllegalArgumentException( "Can not convert object of unknown type to lockable: " + lockableObject.getObjectType() );
            }
        }

    }


    private static Lockable convertNamespaceToLockable( @NonNull LockableObject lockableObject ) {
        LogicalNamespace namespace = (LogicalNamespace) lockableObject;
        return new LockableObjectWrapper( LockablesRegistry.GLOBAL_SCHEMA_LOCKABLE, namespace );
    }


    private static Lockable convertEntityToLockable( @NonNull LockableObject lockableObject ) {
        Entity entity = (Entity) lockableObject;
        Lockable namespace = convertNamespaceToLockable( getNamespaceLockableObjectOfEntity( entity ) );
        return new LockableObjectWrapper( namespace, entity );
    }


    public static LockableObject getNamespaceLockableObjectOfEntity( Entity entity ) {
        return Catalog.getInstance().getSnapshot().getNamespace( entity.getNamespaceId() ).orElseThrow();
    }

}
