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

import com.google.common.collect.ImmutableList;
import lombok.Getter;
import org.polypheny.db.runtime.PolyCollections.PolyDirectory;

@Getter
public abstract class GraphPropertyHolder extends GraphObject {


    public final PolyDirectory properties;
    public final ImmutableList<String> labels;


    public GraphPropertyHolder( long id, GraphObjectType type, PolyDirectory properties, ImmutableList<String> labels ) {
        super( id, type );
        this.properties = properties;
        this.labels = labels;
    }

}
