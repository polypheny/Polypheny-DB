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
import org.polypheny.db.catalog.Catalog;
import org.polypheny.db.catalog.entity.Entity;
import org.polypheny.db.catalog.entity.logical.LogicalNamespace;

public class LockablesRegistry {

    public static final GlobalSchemaLockable GLOBAL_SCHEMA_LOCKABLE = new GlobalSchemaLockable();
    public static final LockablesRegistry INSTANCE = new LockablesRegistry();

    private final ConcurrentHashMap<LockableObject, Lockable> lockables = new ConcurrentHashMap<>();


    public Lockable getOrCreateLockable(@NonNull LockableObject lockableObject ) {
        return lockables.computeIfAbsent( lockableObject, LockableUtils::convertToLockable );
    }
}
