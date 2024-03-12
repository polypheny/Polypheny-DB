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

package org.polypheny.db.languages;

import lombok.Getter;
import org.polypheny.db.catalog.logistic.DataModel;
import org.polypheny.db.interpreter.Node;

/**
 * Container class which should be used to hand different parameters to either {@link NodeToAlgConverter}
 * or when translating native language strings into language-specific {@link Node} implementations
 */
@Getter
public class QueryParameters {

    private final DataModel dataModel;
    private final String query;


    public QueryParameters( String query, DataModel dataModel ) {
        this.dataModel = dataModel;
        this.query = query;
    }

}
