/*
 * The MIT License (MIT)
 *
 * Copyright (c) 2019 Databases and Information Systems Research Group, University of Basel, Switzerland
 *
 * Permission is hereby granted, free of charge, to any person obtaining a
 * copy of this software and associated documentation files (the "Software"), to deal
 * in the Software without restriction, including without limitation the rights
 * to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
 * copies of the Software, and to permit persons to whom the Software is
 * furnished to do so, subject to the following conditions:
 *
 * The above copyright notice and this permission notice shall be included in all
 * copies or substantial portions of the Software.
 *
 * THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
 * IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
 * FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
 * AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
 * LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
 * OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
 * SOFTWARE.
 *
 */

package ch.unibas.dmi.dbis.polyphenydb.catalog.entity.combined;


import ch.unibas.dmi.dbis.polyphenydb.catalog.Catalog.ConstraintType;
import ch.unibas.dmi.dbis.polyphenydb.catalog.entity.CatalogColumn;
import ch.unibas.dmi.dbis.polyphenydb.catalog.entity.CatalogConstraint;
import ch.unibas.dmi.dbis.polyphenydb.catalog.entity.CatalogDatabase;
import ch.unibas.dmi.dbis.polyphenydb.catalog.entity.CatalogForeignKey;
import ch.unibas.dmi.dbis.polyphenydb.catalog.entity.CatalogIndex;
import ch.unibas.dmi.dbis.polyphenydb.catalog.entity.CatalogKey;
import ch.unibas.dmi.dbis.polyphenydb.catalog.entity.CatalogSchema;
import ch.unibas.dmi.dbis.polyphenydb.catalog.entity.CatalogTable;
import java.util.List;
import lombok.Getter;
import lombok.NonNull;


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

}
