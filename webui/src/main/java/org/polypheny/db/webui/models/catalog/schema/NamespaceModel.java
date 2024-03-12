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

import org.polypheny.db.catalog.entity.logical.LogicalNamespace;
import org.polypheny.db.catalog.logistic.DataModel;
import org.polypheny.db.webui.models.catalog.IdEntity;

public class NamespaceModel extends IdEntity {

    public final boolean caseSensitive;

    public final DataModel dataModel;


    public NamespaceModel( long id, String name, DataModel type, boolean caseSensitive ) {
        super( id, name );
        this.dataModel = type;
        this.caseSensitive = caseSensitive;
    }


    public static NamespaceModel from( LogicalNamespace namespace ) {
        return new NamespaceModel( namespace.id, namespace.name, namespace.dataModel, namespace.caseSensitive );
    }

}
