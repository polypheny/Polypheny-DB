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

package org.polypheny.db.webui.models.catalog;


import com.fasterxml.jackson.annotation.JsonProperty;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Optional;
import java.util.stream.Collectors;
import lombok.Data;
import lombok.EqualsAndHashCode;
import lombok.NoArgsConstructor;
import org.jetbrains.annotations.Nullable;
import org.polypheny.db.adapter.Adapter;
import org.polypheny.db.adapter.AdapterManager;
import org.polypheny.db.adapter.DataStore;
import org.polypheny.db.adapter.DataStore.IndexMethodModel;
import org.polypheny.db.adapter.DeployMode;
import org.polypheny.db.catalog.entity.LogicalAdapter;
import org.polypheny.db.catalog.entity.LogicalAdapter.AdapterType;


@EqualsAndHashCode(callSuper = true)
@Data
@NoArgsConstructor
public class AdapterModel extends IdEntity {

    @JsonProperty
    public String adapterName;

    @JsonProperty
    public AdapterType type;

    @JsonProperty
    public Map<String, AdapterSettingValueModel> settings;

    @JsonProperty
    public DeployMode mode;

    @JsonProperty
    public List<IndexMethodModel> indexMethods;


    public AdapterModel(
            @JsonProperty("id") @Nullable Long id,
            @JsonProperty("name") @Nullable String name,
            @JsonProperty("adapterName") String adapterName,
            @JsonProperty("type") AdapterType type,
            @JsonProperty("settings") Map<String, AdapterSettingValueModel> settings,
            @JsonProperty("mode") DeployMode mode,
            @JsonProperty("indexMethods") List<IndexMethodModel> indexMethods ) {
        super( id, name );
        this.adapterName = adapterName;
        this.type = type;
        this.settings = settings;
        this.mode = mode;
        this.indexMethods = indexMethods;
    }


    @Nullable
    public static AdapterModel from( LogicalAdapter adapter ) {
        Map<String, AdapterSettingValueModel> settings = adapter.settings.entrySet().stream().collect( Collectors.toMap( Entry::getKey, s -> AdapterSettingValueModel.from( s.getKey(), s.getValue() ) ) );

        Optional<Adapter<?>> a = AdapterManager.getInstance().getAdapter( adapter.id );
        return a.map( dataStore -> new AdapterModel(
                adapter.id,
                adapter.uniqueName,
                adapter.adapterName,
                adapter.type,
                settings,
                adapter.mode,
                adapter.type == AdapterType.STORE ? ((DataStore<?>) dataStore).getAvailableIndexMethods() : List.of() ) ).orElse( null );

    }


    public record AdapterSettingValueModel(@JsonProperty("name") String name, @JsonProperty("value") String value) {


        public static AdapterSettingValueModel from( String name, String value ) {
            return new AdapterSettingValueModel( name, value );
        }

    }

}
