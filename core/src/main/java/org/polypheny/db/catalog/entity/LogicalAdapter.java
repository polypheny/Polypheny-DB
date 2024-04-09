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

package org.polypheny.db.catalog.entity;


import io.activej.serializer.annotations.Deserialize;
import io.activej.serializer.annotations.Serialize;
import java.io.Serial;
import java.util.HashMap;
import java.util.Map;
import lombok.EqualsAndHashCode;
import lombok.NonNull;
import lombok.Value;
import lombok.experimental.SuperBuilder;
import org.jetbrains.annotations.NotNull;
import org.polypheny.db.adapter.DeployMode;
import org.polypheny.db.type.entity.PolyString;
import org.polypheny.db.type.entity.PolyValue;

@EqualsAndHashCode
@Value
@SuperBuilder(toBuilder = true)
public class LogicalAdapter implements PolyObject {

    @Serial
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
    public Map<String, String> settings;
    @Serialize
    public String adapterTypeName;
    @Serialize
    public DeployMode mode;


    public enum AdapterType {STORE, SOURCE}


    public LogicalAdapter(
            @Deserialize("id") final long id,
            @Deserialize("uniqueName") @NonNull final String uniqueName,
            @Deserialize("adapterName") @NonNull final String adapterName,
            @Deserialize("type") @NonNull final AdapterType adapterType,
            @Deserialize("mode") @NotNull final DeployMode mode,
            @Deserialize("settings") @NonNull final Map<String, String> settings ) {
        this.id = id;
        this.uniqueName = uniqueName;
        this.adapterName = adapterName;
        this.type = adapterType;
        this.settings = new HashMap<>( settings );
        this.adapterTypeName = getAdapterName();
        this.mode = mode;
    }


    // Used for creating ResultSets
    @Override
    public PolyValue[] getParameterArray() {
        return new PolyValue[]{ PolyString.of( uniqueName ) };
    }

}
