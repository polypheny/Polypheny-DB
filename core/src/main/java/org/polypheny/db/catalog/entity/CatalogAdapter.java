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


import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import java.io.Serializable;
import java.util.List;
import java.util.Map;
import lombok.EqualsAndHashCode;
import lombok.NonNull;
import lombok.Value;
import lombok.With;
import org.polypheny.db.adapter.Adapter.AdapterProperties;
import org.polypheny.db.catalog.Adapter;
import org.polypheny.db.catalog.logistic.NamespaceType;

@EqualsAndHashCode
@Value
@With
public class CatalogAdapter implements CatalogObject {

    private static final long serialVersionUID = -6140489767408917639L;

    public long id;
    public String uniqueName;
    public String adapterName;
    public AdapterType type;
    public ImmutableMap<String, String> settings;
    public ImmutableList<NamespaceType> supportedNamespaces;

    public String adapterTypeName;


    public enum AdapterType {STORE, SOURCE}


    public CatalogAdapter(
            final long id,
            @NonNull final String uniqueName,
            @NonNull final String adapterName,
            @NonNull final AdapterType adapterType,
            @NonNull final Map<String, String> settings ) {
        this.id = id;
        this.uniqueName = uniqueName;
        this.adapterName = adapterName;
        this.type = adapterType;
        this.settings = ImmutableMap.copyOf( settings );
        this.supportedNamespaces = ImmutableList.copyOf( createSupportedNamespaces() );
        this.adapterTypeName = getAdapterName();
    }


    private String getAdapterTypeName() {
        // General settings are provided by the annotations of the adapter class
        AdapterProperties annotations = Adapter.fromString( adapterName, type ).getClazz().getAnnotation( AdapterProperties.class );
        return annotations.name();
    }


    private List<NamespaceType> createSupportedNamespaces() {
        // General settings are provided by the annotations of the adapter class
        AdapterProperties annotations = Adapter.fromString( adapterName, type ).getClazz().getAnnotation( AdapterProperties.class );
        return List.of( annotations.supportedNamespaceTypes() );
    }


    // Used for creating ResultSets
    @Override
    public Serializable[] getParameterArray() {
        return new Serializable[]{ uniqueName };
    }

}
