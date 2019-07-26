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

package ch.unibas.dmi.dbis.polyphenydb.catalog.entity;


import ch.unibas.dmi.dbis.polyphenydb.catalog.CatalogManager.Collation;
import ch.unibas.dmi.dbis.polyphenydb.catalog.CatalogManager.Encoding;
import ch.unibas.dmi.dbis.polyphenydb.catalog.CatalogManager.SchemaType;
import lombok.EqualsAndHashCode;
import lombok.NonNull;
import lombok.RequiredArgsConstructor;


/**
 *
 */
@EqualsAndHashCode
public final class CatalogSchema implements CatalogEntity {

    private static final long serialVersionUID = 6130781950959616712L;

    public final long id;
    public final String name;
    public final long databaseId;
    public final String databaseName;
    public final int ownerId;
    public final String ownerName;
    public final Encoding encoding;
    public final Collation collation;
    public final SchemaType schemaType;


    public CatalogSchema( final long id, @NonNull final String name, final long databaseId, @NonNull final String databaseName, final int ownerId, @NonNull final String ownerName, final Encoding encoding, final Collation collation, @NonNull final SchemaType schemaType ) {
        this.id = id;
        this.name = name;
        this.databaseId = databaseId;
        this.databaseName = databaseName;
        this.ownerId = ownerId;
        this.ownerName = ownerName;
        this.encoding = encoding;
        this.collation = collation;
        this.schemaType = schemaType;
    }


    // Used for creating ResultSets
    @Override
    public Object[] getParameterArray() {
        return new Object[]{ name, databaseName, ownerName, CatalogEntity.getEnumNameOrNull( encoding ), CatalogEntity.getEnumNameOrNull( collation ), CatalogEntity.getEnumNameOrNull( schemaType ) };
    }


    @RequiredArgsConstructor
    public class PrimitiveCatalogSchema {

        public final String tableSchem;
        public final String tableCatalog;
        public final String owner;
        public final String encoding;
        public final String collation;
        public final String schemaType;
    }
}
