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
import org.polypheny.db.catalog.Catalog;
import org.polypheny.db.catalog.entity.Entity;
import org.polypheny.db.catalog.entity.logical.LogicalCollection;
import org.polypheny.db.catalog.entity.logical.LogicalNamespace;
import org.polypheny.db.config.RuntimeConfig;
import org.polypheny.db.prepare.Context;
import org.polypheny.db.processing.QueryContext.ParsedQueryContext;
import org.polypheny.db.transaction.locking.Lockable.LockType;

public class LockableUtils {

    public static LockableObject getNamespaceAsLockableObject( Entity entity ) {
        return Catalog.snapshot().getNamespace( entity.getNamespaceId() ).orElseThrow();
    }


    /**
     * This method converts a desired {@link LockableObject} to the one appropriate for the {@link S2plLockingLevel} set
     * in the {@link RuntimeConfig}.
     * <p>
     * This is required to resolve mismatches between the requested and the supported lockable objects. An example:
     * A lock is requested for an entity. The locking level in the config is set to NAMESPACE. In this case, a
     * lockable object for the namespace is returned.
     *
     * @param lockableObject desired lockable object to acquire
     * @return the object actually to be acquired
     */
    public static Lockable deriveLockable( LockableObject lockableObject ) {
        S2plLockingLevel lockingLevel = (S2plLockingLevel) RuntimeConfig.S2PL_LOCKING_LEVEL.getEnum();
        switch ( lockableObject.getLockableObjectType() ) {
            case NAMESPACE -> {
                if ( lockingLevel == S2plLockingLevel.GLOBAL ) {
                    return LockablesRegistry.GLOBAL_SCHEMA_LOCKABLE;
                }
                return LockablesRegistry.INSTANCE.getOrCreateLockable( lockableObject );
            }
            case ENTITY -> {
                if ( lockingLevel == S2plLockingLevel.GLOBAL ) {
                    return LockablesRegistry.GLOBAL_SCHEMA_LOCKABLE;
                }
                if ( lockingLevel == S2plLockingLevel.NAMESPACE ) {
                    LockableObject namespaceLockableObject = LockableUtils.getNamespaceAsLockableObject( (Entity) lockableObject );
                    return LockablesRegistry.INSTANCE.getOrCreateLockable( namespaceLockableObject );
                }
                return LockablesRegistry.INSTANCE.getOrCreateLockable( lockableObject );
            }
            default -> throw new IllegalArgumentException( "Unknown LockableObjectType: " + lockableObject.getLockableObjectType() );
        }
    }


    /**
     * Takes a map of Lockables and updates the {@link LockType} to the passed lock type if it is stricter than the one already contained.
     * Missing entries are added.
     * <p>
     * CAUTION: Lockable and lock type mismatches due to the configured {@link S2plLockingLevel} are NOT resolved.
     *
     * @param lockable lockable to update
     * @param lockType lock type to update to if it is stricter than the already contained one
     * @param lockableMap map containing the current lock types
     */
    private static void updateMapEntry( Lockable lockable, LockType lockType, Map<Lockable, LockType> lockableMap ) {
        LockType currentLockType = lockableMap.get( lockable );
        if ( currentLockType == null || lockType == LockType.EXCLUSIVE ) {
            lockableMap.put( lockable, lockType );
        }
    }


    /**
     * Takes a map of Lockables and updates the entry for a given {@link Entity } to the specified {@link LockType} if it is stricter than the one already contained.
     * <p>
     * Lockable and lock type mismatches due to the configured {@link S2plLockingLevel} ARE resolved.
     * This method first retrieves the entities lockable from the registry or creates a new one if absent.
     * The map entry for this lockable is then updated if appropriate. If the lockable is absent from the map, it is added.
     *
     * @param entity entity of which to update the lockable
     * @param lockType desired lockable type to update to
     * @param currentLockables map of lockables and their lock types
     */
    public static void updateMapEntry( Entity entity, LockType lockType, Map<Lockable, LockType> currentLockables ) {
        Lockable lockable = deriveLockable( entity );
        updateMapEntry( lockable, lockType, currentLockables );
    }


    /**
     * Returns a map containing the {@link GlobalLockable} combined with the specified lock type.
     * <p>
     * Lockable and lock type mismatches due to the configured {@link S2plLockingLevel} ARE resolved.
     *
     * @param lockType desired lock type to be set as the value
     * @return map containing the global lockable with the appropriate lock type
     */
    public static Map<Lockable, LockType> getMapOfGlobalLockable( LockType lockType ) {
        HashMap<Lockable, LockType> lockables = new HashMap<>();
        lockables.put( LockablesRegistry.GLOBAL_SCHEMA_LOCKABLE, lockType );
        return lockables;
    }


    /**
     * Returns a map containing the {@link Lockable} of the specified {@link LockableObject}.
     * <p>
     * Lockable and lock type mismatches due to the configured {@link S2plLockingLevel} ARE resolved.
     *
     * @param lockableObject lockable object to create a lockable of to use as the key
     * @param lockType lock type to use as the value
     * @return map containing the lockable of the lockable object with the specified value
     */
    public static Map<Lockable, LockType> getMapOfLockableFromObject( LockableObject lockableObject, LockType lockType ) {
        HashMap<Lockable, LockType> lockables = new HashMap<>();
        Lockable lockable = deriveLockable( lockableObject );
        LockableUtils.updateMapEntry( lockable, lockType, lockables );
        return lockables;
    }


    /**
     * Returns a map containing the {@link Lockable} of the specified namespace.
     * <p>
     * Lockable and lock type mismatches due to the configured {@link S2plLockingLevel} ARE resolved.
     *
     * @param namespaceName name of the namespace to use as the key
     * @param context context providing the catalog snapshot to retrieve the namespace from
     * @param lockType desired lock type to set as the value
     * @return map containing the appropriate lockable and lock type for the specified namespace
     */
    public static Map<Lockable, LockType> getMapOfNamespaceLockableFromName( String namespaceName, Context context, LockType lockType ) {
        Optional<LogicalNamespace> namespace = context.getSnapshot().getNamespace( namespaceName );
        return namespace.map( logicalNamespace -> getMapOfNamespaceLockable( logicalNamespace, lockType ) ).orElseGet( HashMap::new );
    }


    /**
     * Returns a map containing the {@link Lockable} of the namespace specified in the {@link ParsedQueryContext}.
     * <p>
     * Lockable and lock type mismatches due to the configured {@link S2plLockingLevel} ARE resolved.
     *
     * @param context context providing the catalog snapshot to retrieve the namespace from
     * @param parsedQueryContext contains the id of the namespace to use as the value
     * @param lockType contains the desired lock type to set as the value
     * @return map containing the appropriate lockable and lock type for the specified namespace
     */
    public static Map<Lockable, LockType> getMapOfNamespaceLockableFromContext( Context context, ParsedQueryContext parsedQueryContext, LockType lockType ) {
        long namespaceId = parsedQueryContext.getNamespaceId();
        Optional<LogicalNamespace> namespace = context.getSnapshot().getNamespace( namespaceId );
        return namespace.map( logicalNamespace -> getMapOfNamespaceLockable( logicalNamespace, lockType ) ).orElseGet( HashMap::new );
    }


    /**
     * Return a map containing the {@link Lockable} of the passed namespace.
     * <p>
     * Lockable and lock type mismatches due to the configured {@link S2plLockingLevel} ARE resolved.
     *
     * @param namespace namespace of which to get a lockable and put into the map as the key
     * @param lockType desired lock type to set as the value
     * @return map containing the appropriate lockable and lock type for the specified namespace
     */
    public static Map<Lockable, LockType> getMapOfNamespaceLockable( LogicalNamespace namespace, LockType lockType ) {
        HashMap<Lockable, LockType> lockables = new HashMap<>();
        Lockable lockable = deriveLockable( namespace );
        updateMapEntry( lockable, lockType, lockables );
        return lockables;
    }


    /**
     * Returns a map containing the {@link Lockable} of the collection specified in the {@link ParsedQueryContext}.
     * <p>
     * Lockable and lock type mismatches due to the configured {@link S2plLockingLevel} ARE resolved.
     *
     * @param context context providing the catalog snapshot to retrieve the collection from
     * @param parsedQueryContext contains the id of the collection to use as the value
     * @param lockType contains the desired lock type to set as the value
     * @return map containing the appropriate lockable and lock type for the specified namespace
     */
    public static Map<Lockable, LockType> getMapOfCollectionLockableFromContext( Context context, ParsedQueryContext parsedQueryContext, String collectionName, LockType lockType ) {
        long namespaceId = parsedQueryContext.getNamespaceId();
        Optional<LogicalCollection> collection = context.getSnapshot().doc().getCollection( namespaceId, collectionName );
        return collection.map( logicalCollection -> getMapOfLockableFromObject( logicalCollection, lockType ) ).orElseGet( HashMap::new );
    }

}
