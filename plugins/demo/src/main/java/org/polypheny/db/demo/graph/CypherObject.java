/*
 * Copyright 2019-2026 The Polypheny Project
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

package org.polypheny.db.demo.graph;

import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.stream.Collectors;

public abstract class CypherObject {
    List<Field<?>> queryParts = new ArrayList<>();

    void addQueryPart( Field<?> part ) {
        queryParts.add( part );
    }

    String getQueryPart() {
        return queryParts.stream().map( part -> {
            if ( part.isNull() ) {
                return null;
            }

            return part.get();
        } ).filter( Objects::nonNull ).collect( Collectors.joining(",") );
    }


    abstract String query();
}
