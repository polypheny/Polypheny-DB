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


import com.google.common.collect.ImmutableMap;
import io.activej.serializer.annotations.Deserialize;
import io.activej.serializer.annotations.Serialize;
import java.io.Serial;
import java.util.Map;
import lombok.EqualsAndHashCode;
import lombok.NonNull;
import lombok.Value;
import lombok.experimental.SuperBuilder;
import org.polypheny.db.type.entity.PolyString;
import org.polypheny.db.type.entity.PolyValue;


@EqualsAndHashCode
@Value
@SuperBuilder(toBuilder = true)
public class LogicalQueryInterface implements PolyObject {

    @Serial
    private static final long serialVersionUID = 7212289724539530050L;

    @Serialize
    public long id;
    @Serialize
    public String name;
    @Serialize
    public String clazz;
    @Serialize
    public ImmutableMap<String, String> settings;


    public LogicalQueryInterface(
            @Deserialize("id") final long id,
            @Deserialize("name") @NonNull final String uniqueName,
            @Deserialize("clazz") @NonNull final String clazz,
            @Deserialize("settings") @NonNull final Map<String, String> settings ) {
        this.id = id;
        this.name = uniqueName;
        this.clazz = clazz;
        this.settings = ImmutableMap.copyOf( settings );
    }


    // Used for creating ResultSets
    @Override
    public PolyValue[] getParameterArray() {
        return new PolyValue[]{ PolyString.of( name ) };
    }


}
