/*
 * Copyright 2019-2022 The Polypheny Project
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

package org.polypheny.db.catalog.entity;

import java.io.Serializable;
import javax.annotation.Nullable;


public class CatalogGraphPlacement implements CatalogObject {

    private static final long serialVersionUID = 5889825050034392549L;

    public final int adapterId;
    public final long graphId;
    public final String physicalName;
    public final long partitionId;


    public CatalogGraphPlacement( int adapterId, long graphId, @Nullable String physicalName, long partitionId ) {
        this.adapterId = adapterId;
        this.graphId = graphId;
        this.physicalName = physicalName;
        this.partitionId = partitionId;
    }


    @Override
    public Serializable[] getParameterArray() {
        return new Serializable[0];
    }


    public CatalogGraphPlacement replacePhysicalName( String physicalName ) {
        return new CatalogGraphPlacement( adapterId, graphId, physicalName, partitionId );
    }

}
