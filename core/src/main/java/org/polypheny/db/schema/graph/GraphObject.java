/*
 * Copyright 2019-2022 The Polypheny Project
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

package org.polypheny.db.schema.graph;

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.google.gson.annotations.Expose;
import lombok.Getter;
import org.polypheny.db.tools.ExpressionTransformable;


@Getter
public abstract class GraphObject implements ExpressionTransformable {

    private static final Gson gson = new GsonBuilder().enableComplexMapKeySerialization().excludeFieldsWithoutExposeAnnotation().create();

    @Expose
    public final String id;
    @Expose
    public final GraphObjectType type;

    private final String variableName;


    protected GraphObject( String id, GraphObjectType type, String variableName ) {
        this.id = id;
        this.type = type;
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
