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

package org.polypheny.db.catalog.catalogs;

import io.activej.serializer.annotations.SerializeClass;
import java.util.concurrent.ConcurrentHashMap;
import org.polypheny.db.catalog.entity.allocation.AllocationPartition;
import org.polypheny.db.catalog.entity.allocation.AllocationPlacement;
import org.polypheny.db.catalog.entity.logical.LogicalNamespace;
import org.polypheny.db.catalog.impl.allocation.PolyAllocDocCatalog;
import org.polypheny.db.catalog.impl.allocation.PolyAllocGraphCatalog;
import org.polypheny.db.catalog.impl.allocation.PolyAllocRelCatalog;
import org.polypheny.db.util.Wrapper;

@SerializeClass(subclasses = { PolyAllocDocCatalog.class, PolyAllocGraphCatalog.class, PolyAllocRelCatalog.class })
public interface AllocationCatalog extends Wrapper {

    LogicalNamespace getNamespace();

    ConcurrentHashMap<Long, AllocationPartition> getPartitions();

    ConcurrentHashMap<Long, AllocationPlacement> getPlacements();

}
