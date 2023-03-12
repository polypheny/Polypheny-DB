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

package org.polypheny.db.catalog.snapshot;

import java.util.List;
import org.polypheny.db.catalog.entity.physical.PhysicalCollection;
import org.polypheny.db.catalog.entity.physical.PhysicalEntity;
import org.polypheny.db.catalog.entity.physical.PhysicalGraph;
import org.polypheny.db.catalog.entity.physical.PhysicalTable;

public interface PhysicalSnapshot {

    //// PHYSICAL ENTITIES

    PhysicalTable getPhysicalTable( long id );

    PhysicalTable getPhysicalTable( long logicalId, long adapterId );

    PhysicalCollection getPhysicalCollection( long id );

    PhysicalCollection getPhysicalCollection( long logicalId, long adapterId );


    PhysicalGraph getPhysicalGraph( long id );

    PhysicalGraph getPhysicalGraph( long logicalId, long adapterId );

    List<PhysicalEntity> getPhysicalsOnAdapter( long adapterId );

}
