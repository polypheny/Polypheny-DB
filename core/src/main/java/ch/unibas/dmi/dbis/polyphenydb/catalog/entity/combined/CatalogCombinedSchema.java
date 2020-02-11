/*
 * Copyright 2019-2020 The Polypheny Project
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

package ch.unibas.dmi.dbis.polyphenydb.catalog.entity.combined;


import ch.unibas.dmi.dbis.polyphenydb.catalog.entity.CatalogDatabase;
import ch.unibas.dmi.dbis.polyphenydb.catalog.entity.CatalogSchema;
import ch.unibas.dmi.dbis.polyphenydb.catalog.entity.CatalogUser;
import java.util.List;
import lombok.Getter;
import lombok.NonNull;


public class CatalogCombinedSchema implements CatalogCombinedEntity {

    private static final long serialVersionUID = 965922217831463449L;


    @Getter
    private final CatalogSchema schema;
    @Getter
    private final List<CatalogCombinedTable> tables;
    @Getter
    private CatalogDatabase database;
    @Getter
    private final CatalogUser owner;


    public CatalogCombinedSchema( @NonNull CatalogSchema schema, @NonNull List<CatalogCombinedTable> tables, @NonNull CatalogDatabase database, @NonNull CatalogUser owner ) {
        this.schema = schema;
        this.tables = tables;
        this.database = database;
        this.owner = owner;
    }

}
