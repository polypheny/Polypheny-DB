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

package org.polypheny.db.cypher.cypher2alg;

import java.util.ArrayList;
import java.util.List;
import lombok.Getter;
import lombok.Setter;
import org.polypheny.db.catalog.Catalog.NamespaceType;
import org.polypheny.db.languages.QueryParameters;


@Getter
public class CypherQueryParameters extends QueryParameters {

    List<String> nodeLabels = new ArrayList<>();
    List<String> relationshipLabels = new ArrayList<>();
    final String databaseName;
    @Setter
    Long databaseId;
    final boolean fullGraph;


    public CypherQueryParameters( String query, NamespaceType namespaceType, String databaseName ) {
        super( query, namespaceType );
        this.databaseName = databaseName;
        this.fullGraph = false;
    }


    public CypherQueryParameters( String databaseName ) {
        super( "*", NamespaceType.GRAPH );
        this.databaseName = databaseName;
        this.fullGraph = true;
    }

}
