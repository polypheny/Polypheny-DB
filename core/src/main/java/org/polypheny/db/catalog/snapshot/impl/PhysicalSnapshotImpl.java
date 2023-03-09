/*
 * Copyright 2019-2023 The Polypheny Project
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

package org.polypheny.db.catalog.snapshot.impl;

import java.util.List;
import java.util.Map;
import org.polypheny.db.catalog.catalogs.PhysicalCatalog;
import org.polypheny.db.catalog.entity.physical.PhysicalCollection;
import org.polypheny.db.catalog.entity.physical.PhysicalEntity;
import org.polypheny.db.catalog.entity.physical.PhysicalGraph;
import org.polypheny.db.catalog.entity.physical.PhysicalTable;
import org.polypheny.db.catalog.snapshot.PhysicalSnapshot;

public class PhysicalSnapshotImpl implements PhysicalSnapshot {

    public PhysicalSnapshotImpl( Map<Long, PhysicalCatalog> physicalCatalogs ) {
    }


    @Override
    public PhysicalTable getPhysicalTable( long id ) {
        return null;
    }


    @Override
    public PhysicalTable getPhysicalTable( long logicalId, long adapterId ) {
        return null;
    }


    @Override
    public PhysicalCollection getPhysicalCollection( long id ) {
        return null;
    }


    @Override
    public PhysicalCollection getPhysicalCollection( long logicalId, long adapterId ) {
        return null;
    }


    @Override
    public PhysicalGraph getPhysicalGraph( long id ) {
        return null;
    }


    @Override
    public PhysicalGraph getPhysicalGraph( long logicalId, long adapterId ) {
        return null;
    }


    @Override
    public List<PhysicalEntity<?>> getPhysicalsOnAdapter( long adapterId ) {
        return null;
    }

}
