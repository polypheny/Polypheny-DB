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

package org.polypheny.db.type.entity.graph;

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import io.activej.serializer.annotations.Serialize;
import lombok.EqualsAndHashCode;
import lombok.Value;
import lombok.experimental.NonFinal;
import org.polypheny.db.type.PolyType;
import org.polypheny.db.type.entity.PolyString;
import org.polypheny.db.type.entity.PolyValue;


@EqualsAndHashCode(callSuper = true)
@Value
@NonFinal
public abstract class GraphObject extends PolyValue {

    private static final Gson gson = new GsonBuilder().enableComplexMapKeySerialization().excludeFieldsWithoutExposeAnnotation().create();

    @Serialize
    public PolyString id;

    @Serialize
    public PolyString variableName;


    protected GraphObject( PolyString id, PolyType type, PolyString variableName, boolean nullable ) {
        super( type, nullable );
        this.id = id;
        this.variableName = variableName;
    }


    public String toJson() {
        return gson.toJson( this );
    }


    public enum GraphObjectType {
        GRAPH,
        NODE,
        EDGE,
        SEGMENT,
        PATH
    }

}
