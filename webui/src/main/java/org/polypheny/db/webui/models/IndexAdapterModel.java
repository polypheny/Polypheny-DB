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

package org.polypheny.db.webui.models;

import java.util.List;
import lombok.AllArgsConstructor;
import lombok.EqualsAndHashCode;
import lombok.Value;
import org.jetbrains.annotations.Nullable;
import org.polypheny.db.adapter.DataStore;
import org.polypheny.db.webui.models.catalog.IdEntity;

@EqualsAndHashCode(callSuper = true)
@Value
public class IndexAdapterModel extends IdEntity {

    public List<IndexMethodModel> indexMethodModels;


    public IndexAdapterModel( @Nullable Long id, @Nullable String name, List<IndexMethodModel> indexMethodModels ) {
        super( id, name );
        this.indexMethodModels = indexMethodModels;
    }


    public static IndexAdapterModel from( DataStore<?> store ) {
        return new IndexAdapterModel( store.adapterId, store.getUniqueName(), store.getAvailableIndexMethods().stream().map( IndexAdapterModel.IndexMethodModel::from ).toList() );
    }


    @AllArgsConstructor
    public static class IndexMethodModel {

        public String name;
        public String displayName;


        public static IndexMethodModel from( DataStore.IndexMethodModel index ) {
            return new IndexMethodModel( index.name(), index.displayName() );
        }

    }

}
