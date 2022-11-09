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


public class CatalogCollectionMapping implements CatalogObject {

    private static final long serialVersionUID = -5589631288987497205L;

    public final long collectionId;
    public final long idId;
    public final long dataId;


    public CatalogCollectionMapping( long collectionId, long idId, long dataId ) {
        this.collectionId = collectionId;
        this.idId = idId;
        this.dataId = dataId;
    }


    @Override
    public Serializable[] getParameterArray() {
        return new Serializable[0];
    }

}
