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


public class CatalogCollectionPlacement implements CatalogObject {

    private static final long serialVersionUID = 4227137255905904785L;

    public final int adapter;
    public final long collectionId;
    public final String physicalName;
    public final long id;
    public final String physicalNamespaceName;


    public CatalogCollectionPlacement( int adapterId, long collectionId, @Nullable String physicalName, String physicalNamespaceName, long id ) {
        this.adapter = adapterId;
        this.collectionId = collectionId;
        this.physicalName = physicalName;
        this.physicalNamespaceName = physicalNamespaceName;
        this.id = id;
    }


    @Override
    public Serializable[] getParameterArray() {
        return new Serializable[0];
    }

}
