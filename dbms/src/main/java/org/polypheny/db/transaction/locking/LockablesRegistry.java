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

import java.util.concurrent.ConcurrentHashMap;
import lombok.NonNull;
import org.polypheny.db.catalog.entity.Entity;
import org.polypheny.db.catalog.entity.logical.LogicalNamespace;

public class LockablesRegistry {

    public static final GlobalLockable GLOBAL_SCHEMA_LOCKABLE = new GlobalLockable();
    public static final LockablesRegistry INSTANCE = new LockablesRegistry();

    private final ConcurrentHashMap<LockableObject, Lockable> lockables = new ConcurrentHashMap<>();


    /**
     * Returns a {@link Lockable} representing a lock on the passed {@link LockableObject}. The {@link Lockable} is either
     * retrieved from cache or created if absent.
     *
     * @param lockableObject object to create a lockable instance of
     * @return lockable instance representing a lock on the passed lockable object.
     */
    public Lockable getOrCreateLockable( @NonNull LockableObject lockableObject ) {
        return lockables.computeIfAbsent( lockableObject, LockablesRegistry::convertToLockable );
    }


    /**
     * Converts a {@link LockableObject} to a {@link Lockable} which can be acquired by a transaction.
     * This function is only to be used by the LockablesRegistry which caches the {@link Lockable}s after conversion.
     *
     * @param lockableObject
     * @return lockable instance representing a lock on the passed lockable object.
     */
    private static Lockable convertToLockable( @NonNull LockableObject lockableObject ) {
        switch ( lockableObject.getLockableObjectType() ) {
            case NAMESPACE -> {
                return convertToLockable( (LogicalNamespace) lockableObject );
            }

            case ENTITY -> {
                return convertToLockable( (Entity) lockableObject );
            }

            default -> throw new IllegalArgumentException( "Can not convert object of unknown type to lockable: " + lockableObject.getLockableObjectType() );
        }
    }


    /**
     * Converts a {@link LogicalNamespace} to a {@link Lockable} which can be acquired by a transaction.
     * This function is only to be used by the LockablesRegistry which caches the {@link Lockable}s after conversion.
     *
     * @param namespace the logical namespace of which to create a lockable
     * @return lockable instance representing a lock on the passed namespace.
     */
    private static Lockable convertToLockable( @NonNull LogicalNamespace namespace ) {
        return new LockableObjectWrapper( LockablesRegistry.GLOBAL_SCHEMA_LOCKABLE, namespace );
    }


    /**
     * Converts an {@link Entity} to a {@link Lockable} which can be acquired by a transaction.
     * This function is only to be used by the LockablesRegistry which caches the {@link Lockable}s after conversion.
     *
     * @param entity the entity of which to create a lockable
     * @return lockable instance representing a lock on the passed entity.
     */
    private static Lockable convertToLockable( @NonNull Entity entity ) {
        Lockable namespace = LockablesRegistry.INSTANCE.getOrCreateLockable( LockableUtils.getNamespaceAsLockableObject( entity ) );
        return new LockableObjectWrapper( namespace, entity );
    }

}
