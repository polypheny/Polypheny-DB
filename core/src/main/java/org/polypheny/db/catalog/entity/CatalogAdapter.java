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

package org.polypheny.db.catalog.entity;


import com.google.common.collect.ImmutableMap;
import io.activej.serializer.annotations.Deserialize;
import io.activej.serializer.annotations.Serialize;
import java.io.Serializable;
import java.util.Map;
import lombok.EqualsAndHashCode;
import lombok.NonNull;
import lombok.Value;
import lombok.experimental.SuperBuilder;
import org.polypheny.db.adapter.annotations.AdapterProperties;
import org.polypheny.db.adapter.java.AdapterTemplate;

@EqualsAndHashCode
@Value
@SuperBuilder(toBuilder = true)
public class CatalogAdapter implements CatalogObject {

    private static final long serialVersionUID = -6140489767408917639L;

    @Serialize
    public long id;
    @Serialize
    public String uniqueName;
    @Serialize
    public String adapterName;
    @Serialize
    public AdapterType type;
    @Serialize
    public ImmutableMap<String, String> settings;

    @Serialize
    public String adapterTypeName;


    public enum AdapterType {STORE, SOURCE}


    public CatalogAdapter(
            @Deserialize("id") final long id,
            @Deserialize("uniqueName") @NonNull final String uniqueName,
            @Deserialize("adapterName") @NonNull final String adapterName,
            @Deserialize("type") @NonNull final AdapterType adapterType,
            @Deserialize("settings") @NonNull final Map<String, String> settings ) {
        this.id = id;
        this.uniqueName = uniqueName;
        this.adapterName = adapterName;
        this.type = adapterType;
        this.settings = ImmutableMap.copyOf( settings );
        this.adapterTypeName = getAdapterName();
    }


    private String getAdapterTypeName() {
        // General settings are provided by the annotations of the adapter class
        AdapterProperties annotations = AdapterTemplate.fromString( adapterName, type ).getClazz().getAnnotation( AdapterProperties.class );
        return annotations.name();
    }



    // Used for creating ResultSets
    @Override
    public Serializable[] getParameterArray() {
        return new Serializable[]{ uniqueName };
    }

}
