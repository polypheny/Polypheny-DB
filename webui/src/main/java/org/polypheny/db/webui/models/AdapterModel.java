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

package org.polypheny.db.webui.models;


import java.util.Map;
import lombok.EqualsAndHashCode;
import lombok.Value;
import lombok.experimental.NonFinal;
import org.jetbrains.annotations.Nullable;
import org.polypheny.db.adapter.AbstractAdapterSetting;
import org.polypheny.db.catalog.entity.CatalogAdapter;
import org.polypheny.db.catalog.entity.CatalogAdapter.AdapterType;
import org.polypheny.db.catalog.exceptions.GenericRuntimeException;
import org.polypheny.db.webui.models.catalog.IdEntity;


@EqualsAndHashCode(callSuper = true)
@Value
@NonFinal
public abstract class AdapterModel extends IdEntity {

    public String adapterName;
    public AdapterType type;
    public Map<String, AbstractAdapterSetting> settings;
    public boolean persistent;


    public AdapterModel(
            @Nullable Long id,
            @Nullable String name,
            String adapterName,
            AdapterType type,
            Map<String, AbstractAdapterSetting> settings,
            boolean persistent ) {
        super( id, name );
        this.adapterName = adapterName;
        this.type = type;
        this.settings = settings;
        this.persistent = persistent;
    }


    public static AdapterModel from( CatalogAdapter adapter ) {
        switch ( adapter.type ) {
            case STORE:
                return new StoreModel(
                        adapter.id,
                        adapter.uniqueName,
                        adapter.adapterName,
                        adapter.type,
                        Map.of(),
                        true );
            case SOURCE:
                return new SourceModel(
                        adapter.id,
                        adapter.uniqueName,
                        adapter.adapterName,
                        adapter.type,
                        Map.of(),
                        true,
                        false );
            default:
                throw new GenericRuntimeException( "Type of adapter is not known" );
        }
    }

}
