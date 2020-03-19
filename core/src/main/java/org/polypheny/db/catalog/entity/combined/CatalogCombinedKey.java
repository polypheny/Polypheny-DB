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
import org.polypheny.db.catalog.Catalog.ConstraintType;
import org.polypheny.db.catalog.entity.CatalogColumn;
import org.polypheny.db.catalog.entity.CatalogConstraint;
import org.polypheny.db.catalog.entity.CatalogDatabase;
import org.polypheny.db.catalog.entity.CatalogForeignKey;
import org.polypheny.db.catalog.entity.CatalogIndex;
import org.polypheny.db.catalog.entity.CatalogKey;
import org.polypheny.db.catalog.entity.CatalogSchema;
import org.polypheny.db.catalog.entity.CatalogTable;


public class CatalogCombinedKey implements CatalogCombinedEntity {

    private static final long serialVersionUID = 149685644041906885L;

    @Getter
    private CatalogKey key;

    @Getter
    private List<CatalogColumn> columns;

    @Getter
    private CatalogTable table;
    @Getter
    private CatalogSchema schema;
    @Getter
    private CatalogDatabase database;

    @Getter
    private List<CatalogForeignKey> foreignKeys;
    @Getter
    private List<CatalogIndex> indexes;
    @Getter
    private List<CatalogConstraint> constraints;

    @Getter
    private List<CatalogForeignKey> referencedBy;

    @Getter
    private final boolean isPrimaryKey;
    @Getter
    private final int uniqueCount;


    public CatalogCombinedKey(
            @NonNull CatalogKey key,
            @NonNull List<CatalogColumn> columns,
            @NonNull CatalogTable table,
            @NonNull CatalogSchema schema,
            @NonNull CatalogDatabase database,
            @NonNull List<CatalogForeignKey> foreignKeys,
            @NonNull List<CatalogIndex> indexes,
            @NonNull List<CatalogConstraint> constraints,
            @NonNull List<CatalogForeignKey> referencedBy ) {
        this.key = key;
        this.columns = columns;
        this.table = table;
        this.schema = schema;
        this.database = database;
        this.foreignKeys = foreignKeys;
        this.indexes = indexes;
        this.constraints = constraints;
        this.referencedBy = referencedBy;
        if ( table.primaryKey != null ) {
            this.isPrimaryKey = table.primaryKey == key.id;
        } else {
            this.isPrimaryKey = false;
        }
        this.uniqueCount = computeUniqueCount();
    }


    private int computeUniqueCount() {
        int count = 0;
        if ( isPrimaryKey ) {
            count++;
        }

        for ( CatalogConstraint constraint : constraints ) {
            if ( constraint.type == ConstraintType.UNIQUE ) {
                count++;
            }
        }

        for ( CatalogIndex index : indexes ) {
            if ( index.unique ) {
                count++;
            }
        }

        return count;
    }


    @Override
    public String toString() {
        return "[ columns:" + columns.toString() + " ]";
    }
}
