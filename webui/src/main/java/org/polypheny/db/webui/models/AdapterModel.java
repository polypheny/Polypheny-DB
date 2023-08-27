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
import org.jetbrains.annotations.Nullable;
import org.polypheny.db.adapter.AbstractAdapterSetting;
import org.polypheny.db.catalog.entity.CatalogAdapter;
import org.polypheny.db.webui.models.catalog.IdEntity;


@EqualsAndHashCode(callSuper = true)
@Value
public class AdapterModel extends IdEntity {

    public String uniqueName;
    public String adapterName;
    public String adapterType;
    public Map<String, AbstractAdapterSetting> settings;


    public AdapterModel( @Nullable Long id, @Nullable String name, String uniqueName, String adapterName, String adapterType, Map<String, AbstractAdapterSetting> settings ) {
        super( id, name );
        this.uniqueName = uniqueName;
        this.adapterName = adapterName;
        this.adapterType = adapterType;
        this.settings = settings;
    }


    public static AdapterModel from( CatalogAdapter adapter ) {
        return new AdapterModel( adapter.id, adapter.uniqueName, adapter.uniqueName, adapter.adapterName, adapter.adapterTypeName, Map.of() );
    }

}
