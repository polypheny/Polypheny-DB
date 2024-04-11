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

package org.polypheny.db.webui.models.catalog.schema;

import com.fasterxml.jackson.annotation.JsonProperty;
import lombok.EqualsAndHashCode;
import lombok.Value;
import org.polypheny.db.catalog.entity.logical.LogicalNamespace;
import org.polypheny.db.catalog.logistic.DataModel;
import org.polypheny.db.webui.models.catalog.IdEntity;

@EqualsAndHashCode(callSuper = true)
@Value
public class NamespaceModel extends IdEntity {

    @JsonProperty
    public boolean caseSensitive;

    @JsonProperty
    public DataModel dataModel;


    public NamespaceModel(
            @JsonProperty("id") long id,
            @JsonProperty("name") String name,
            @JsonProperty("type") DataModel type,
            @JsonProperty("caseSensitive") boolean caseSensitive ) {
        super( id, name );
        this.dataModel = type;
        this.caseSensitive = caseSensitive;
    }


    public static NamespaceModel from( LogicalNamespace namespace ) {
        return new NamespaceModel( namespace.id, namespace.name, namespace.dataModel, namespace.caseSensitive );
    }

}
