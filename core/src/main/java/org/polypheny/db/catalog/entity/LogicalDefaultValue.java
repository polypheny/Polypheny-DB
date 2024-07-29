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
import io.activej.serializer.annotations.SerializeNullable;
import java.io.Serializable;
import lombok.EqualsAndHashCode;
import lombok.NonNull;
import org.polypheny.db.type.PolyType;
import org.polypheny.db.type.entity.PolyValue;


@EqualsAndHashCode
public class LogicalDefaultValue implements Serializable {

    private static final long serialVersionUID = 6085682952587659184L;

    @Serialize
    public final long fieldId;
    @Serialize
    public final PolyType type;

    public final PolyValue value;
    @Serialize
    @SerializeNullable
    public final String typedJson;

    @Serialize
    public final String functionName;


    public LogicalDefaultValue(
            @Deserialize("fieldId") final long fieldId,
            @Deserialize("type") @NonNull final PolyType type,
            @Deserialize("typedJson") final String typedJson,
            @Deserialize("functionName") final String functionName ) {
        this.fieldId = fieldId;
        this.type = type;
        this.value = PolyValue.deserialize( typedJson );
        this.typedJson = typedJson;
        this.functionName = functionName;
    }


    public LogicalDefaultValue(
            final long fieldId,
            @NonNull final PolyType type,
            final PolyValue value,
            final String functionName ) {
        this.fieldId = fieldId;
        this.type = type;
        this.value = value;
        this.typedJson = value.toTypedJson();
        this.functionName = functionName;
    }


}
