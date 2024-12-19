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
import lombok.NonNull;
import org.polypheny.db.catalog.Catalog;
import org.polypheny.db.catalog.entity.Entity;
import org.polypheny.db.catalog.entity.logical.LogicalCollection;
import org.polypheny.db.catalog.entity.logical.LogicalNamespace;
import org.polypheny.db.config.RuntimeConfig;
import org.polypheny.db.prepare.Context;
import org.polypheny.db.processing.QueryContext.ParsedQueryContext;
import org.polypheny.db.transaction.locking.Lockable.LockType;

public class LockableUtils {

    public static Lockable convertToLockable( @NonNull LockableObject lockableObject ) {
        switch ( lockableObject.getLockableObjectType() ) {
            case NAMESPACE -> {
                return convertNamespaceToLockable( lockableObject );
            }

            case ENTITY -> {
                return convertEntityToLockable( lockableObject );
            }

            default -> throw new IllegalArgumentException( "Can not convert object of unknown type to lockable: " + lockableObject.getLockableObjectType() );
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


    public static void updateMapOfDerivedLockables( Entity entity, LockType lockType, Map<Lockable, LockType> currentLockables ) {
        switch ( (S2plLockingLevel) RuntimeConfig.S2PL_LOCKING_LEVEL.getEnum() ) {
            case GLOBAL -> updateLockableMapEntry( LockablesRegistry.GLOBAL_SCHEMA_LOCKABLE, lockType, currentLockables );
            case NAMESPACE -> {
                Lockable lockable = LockablesRegistry.INSTANCE.getOrCreateLockable( LockableUtils.getNamespaceLockableObjectOfEntity( entity ) );
                updateLockableMapEntry( lockable, lockType, currentLockables );
            }
            case ENTITY -> {
                Lockable lockable = LockablesRegistry.INSTANCE.getOrCreateLockable( entity );
                updateLockableMapEntry( lockable, lockType, currentLockables );
            }
        }
    }


    public static void updateMapOfDerivedLockables( LockableObject lockableObject, LockType lockType, Map<Lockable, LockType> currentLockables ) {
        switch ( lockableObject.getLockableObjectType() ) {
            case NAMESPACE -> {
                S2plLockingLevel lockingLevel = (S2plLockingLevel) RuntimeConfig.S2PL_LOCKING_LEVEL.getEnum();
                if ( lockingLevel == S2plLockingLevel.GLOBAL ) {
                    updateLockableMapEntry( LockablesRegistry.GLOBAL_SCHEMA_LOCKABLE, lockType, currentLockables );
                    return;
                }
                // this always returns a lockable on namespace level as we checked the lockable object type
                Lockable lockable = LockablesRegistry.INSTANCE.getOrCreateLockable( lockableObject );
                updateLockableMapEntry( lockable, lockType, currentLockables );
            }
            case ENTITY -> updateMapOfDerivedLockables( (Entity) lockableObject, lockType, currentLockables );
        }
    }


    private static void updateLockableMapEntry( Lockable lockable, LockType lockType, Map<Lockable, LockType> currentLockables ) {
        LockType currentLockType = currentLockables.get( lockable );
        if ( currentLockType == null || lockType == LockType.EXCLUSIVE ) {
            currentLockables.put( lockable, lockType );
        }
    }

    public static Map<Lockable, LockType> getMapWithGlobalLockable(LockType lockType) {
        HashMap<Lockable, LockType> lockableObjects = new HashMap<>();
        lockableObjects.put( LockablesRegistry.GLOBAL_SCHEMA_LOCKABLE, lockType );
        return lockableObjects;
    }

    public static Map<Lockable, LockType> getMapOfNamespaceLockable(String namespaceName, Context context, LockType lockType) {
        Optional<LogicalNamespace> logicalNamespace = context.getSnapshot().getNamespace( namespaceName );
        HashMap<Lockable, LockType> lockableObjects = new HashMap<>();
        logicalNamespace.ifPresent( n -> LockableUtils.updateMapOfDerivedLockables( n, lockType, lockableObjects ) );
        return lockableObjects;
    }

    public static Map<Lockable, LockType> getMapOfNamespaceLockableFromContext(Context context, ParsedQueryContext parsedQueryContext, LockType lockType) {
        long namespaceId = parsedQueryContext.getNamespaceId();
        LogicalNamespace namespace = context.getSnapshot().getNamespace( namespaceId ).orElseThrow();
        return getMapOfLockableFromObject( namespace, lockType );
    }

    public static Map<Lockable, LockType> getMapOfCollectionLockableFromContext(Context context, ParsedQueryContext parsedQueryContext, String collectionName, LockType lockType) {
        long namespaceId = parsedQueryContext.getNamespaceId();
        LogicalCollection collection = context.getSnapshot().doc().getCollection( namespaceId, collectionName ).orElseThrow();
        return getMapOfLockableFromObject( collection, lockType );
    }

    public static Map<Lockable, LockType> getMapOfLockableFromObject(LockableObject lockableObject, LockType lockType) {
        HashMap<Lockable, LockType> lockableObjects = new HashMap<>();
        LockableUtils.updateMapOfDerivedLockables( lockableObject, lockType, lockableObjects );
        return lockableObjects;
    }



}
