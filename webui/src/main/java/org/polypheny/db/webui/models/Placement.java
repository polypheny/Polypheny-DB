/*
 * Copyright 2019-2020 The Polypheny Project
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

package org.polypheny.db.webui.models;


import java.util.ArrayList;
import java.util.List;
import lombok.AllArgsConstructor;
import org.polypheny.db.catalog.entity.CatalogColumnPlacement;


/**
 * A model for the UI, modelling the placements of a table
 */
public class Placement {

    Throwable exception;
    List<Store> stores = new ArrayList<>();

    public Placement () {}

    public Placement ( final Throwable exception ) {
        this.exception = exception;
    }

    public Placement addStore ( final Store s ) {
        if( s.columnPlacements.size() > 0 ){
            this.stores.add( s );
        }
        return this;
    }

    @AllArgsConstructor
    public static class Store {
        private final String uniqueName;
        private final String adapterName;
        private final boolean dataReadOnly;
        private final boolean schemaReadOnly;
        private final List<CatalogColumnPlacement> columnPlacements;
    }
}
