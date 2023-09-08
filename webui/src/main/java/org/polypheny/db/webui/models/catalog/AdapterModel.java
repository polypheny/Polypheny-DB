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

package org.polypheny.db.webui.models.catalog;


import java.util.Map;
import java.util.Map.Entry;
import java.util.stream.Collectors;
import lombok.Data;
import lombok.EqualsAndHashCode;
import lombok.NoArgsConstructor;
import lombok.Value;
import org.jetbrains.annotations.Nullable;
import org.polypheny.db.adapter.DeployMode;
import org.polypheny.db.catalog.entity.CatalogAdapter;
import org.polypheny.db.catalog.entity.CatalogAdapter.AdapterType;
import org.polypheny.db.catalog.exceptions.GenericRuntimeException;


@EqualsAndHashCode(callSuper = true)
@Data
@NoArgsConstructor
public class AdapterModel extends IdEntity {

    public String adapterName;
    public AdapterType type;
    public Map<String, AdapterSettingValueModel> settings;
    public DeployMode mode;


    public AdapterModel(
            @Nullable Long id,
            @Nullable String name,
            String adapterName,
            AdapterType type,
            Map<String, AdapterSettingValueModel> settings,
            DeployMode mode ) {
        super( id, name );
        this.adapterName = adapterName;
        this.type = type;
        this.settings = settings;
        this.mode = mode;
    }


    public static AdapterModel from( CatalogAdapter adapter ) {
        Map<String, AdapterSettingValueModel> settings = adapter.settings.entrySet().stream().collect( Collectors.toMap( Entry::getKey, s -> AdapterSettingValueModel.from( s.getKey(), s.getValue() ) ) );
        return new AdapterModel(
                adapter.id,
                adapter.uniqueName,
                adapter.adapterName,
                adapter.type,
                settings,
                adapter.mode );
    }


    @Value
    public static class AdapterSettingValueModel {

        String name;
        String value;


        public static AdapterSettingValueModel from( String name, String value ) {
            return new AdapterSettingValueModel( name, value );
        }

    }

}
