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
import java.util.UUID;
import org.polypheny.db.catalog.entity.logical.IdentityColumn;
import org.polypheny.db.catalog.entity.logical.IdentityColumn.Mode;
import lombok.EqualsAndHashCode;
import lombok.NonNull;
import org.polypheny.db.type.PolyType;
import org.polypheny.db.type.entity.PolyValue;


@EqualsAndHashCode
public class LogicalDefaultValue implements Serializable {

    private static final long serialVersionUID = 6085682952587659184L;
    // The existing default record format can carry a generated default without changing the catalog binary layout.
    private static final String IDENTITY = "identity-v1";

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
    public final IdentityColumn identity;


    public LogicalDefaultValue(
            @Deserialize("fieldId") final long fieldId,
            @Deserialize("type") @NonNull final PolyType type,
            @Deserialize("typedJson") final String typedJson,
            @Deserialize("functionName") final String functionName ) {
        this.fieldId = fieldId;
        this.type = type;
        this.identity = IDENTITY.equals( functionName ) ? parseIdentity( typedJson ) : null;
        this.value = identity == null ? PolyValue.fromJson( typedJson ) : null;
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
        this.identity = null;
        this.functionName = functionName;
    }


    public static LogicalDefaultValue forIdentity( long fieldId, PolyType type, IdentityColumn identity ) {
        String encoded = String.join( ",",
                identity.id().toString(),
                identity.mode().name(),
                Long.toString( identity.start() ),
                Long.toString( identity.increment() ),
                Long.toString( identity.min() ),
                Long.toString( identity.max() ),
                Boolean.toString( identity.cycle() ),
                Long.toString( identity.initialNext() ),
                Boolean.toString( identity.initiallyExhausted() ) );
        return new LogicalDefaultValue( fieldId, type, encoded, IDENTITY );
    }


    private static IdentityColumn parseIdentity( String encoded ) {
        String[] fields = encoded.split( ",", -1 );
        return new IdentityColumn(
                UUID.fromString( fields[0] ),
                Mode.valueOf( fields[1] ),
                Long.parseLong( fields[2] ),
                Long.parseLong( fields[3] ),
                Long.parseLong( fields[4] ),
                Long.parseLong( fields[5] ),
                Boolean.parseBoolean( fields[6] ),
                Long.parseLong( fields[7] ),
                Boolean.parseBoolean( fields[8] ) );
    }

}
