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

package org.polypheny.db.catalog.entity.combined;


import java.util.List;
import lombok.Getter;
import lombok.NonNull;
import org.polypheny.db.catalog.entity.CatalogDatabase;
import org.polypheny.db.catalog.entity.CatalogSchema;
import org.polypheny.db.catalog.entity.CatalogUser;


public class CatalogCombinedDatabase implements CatalogCombinedEntity {

    private static final long serialVersionUID = 8705987903992498470L;

    @Getter
    private final CatalogDatabase database;
    @Getter
    private final List<CatalogCombinedSchema> schemas;
    @Getter
    private final CatalogSchema defaultSchema;
    @Getter
    private final CatalogUser owner;


    public CatalogCombinedDatabase( @NonNull CatalogDatabase database, @NonNull List<CatalogCombinedSchema> schemas, CatalogSchema defaultSchema, @NonNull CatalogUser owner ) {
        this.database = database;
        this.schemas = schemas;
        this.owner = owner;
        this.defaultSchema = defaultSchema;
    }


    @Override
    public String toString() {
        return "[ id:" + database.id + ", name: " + database.name + ", schemas:" + schemas.toString() + "]";
    }
}
