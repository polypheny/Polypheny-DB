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


import ch.unibas.dmi.dbis.polyphenydb.PolySqlType;
import ch.unibas.dmi.dbis.polyphenydb.catalog.CatalogManager.Collation;
import ch.unibas.dmi.dbis.polyphenydb.catalog.CatalogManager.Encoding;
import lombok.EqualsAndHashCode;
import lombok.NonNull;
import lombok.RequiredArgsConstructor;


/**
 *
 */
@EqualsAndHashCode
public final class CatalogColumn implements CatalogEntity {

    private static final long serialVersionUID = -6566756853822620430L;

    public final long id;
    public final String name;
    public final long tableId;
    public final String tableName;
    public final long schemaId;
    public final String schemaName;
    public final long databaseId;
    public final String databaseName;
    public final int position;
    public final PolySqlType type;
    public final Integer length;
    public final Integer precision;
    public final boolean nullable;
    public final Encoding encoding;
    public final Collation collation;
    public final boolean forceDefault;


    public CatalogColumn( final long id, @NonNull final String name, final long tableId, @NonNull final String tableName, final long schemaId, @NonNull final String schemaName, final long databaseId, @NonNull final String databaseName, final int position, @NonNull final PolySqlType type, final Integer length, final Integer precision, final boolean nullable, final Encoding encoding, final Collation collation, final boolean forceDefault ) {
        this.id = id;
        this.name = name;
        this.tableId = tableId;
        this.tableName = tableName;
        this.schemaId = schemaId;
        this.schemaName = schemaName;
        this.databaseId = databaseId;
        this.databaseName = databaseName;
        this.position = position;
        this.type = type;
        this.length = length;
        this.precision = precision;
        this.nullable = nullable;
        this.encoding = encoding;
        this.collation = collation;
        this.forceDefault = forceDefault;
    }


    @Override
    public Object[] getParameterArray() {
        return new Object[]{ name, tableName, schemaName, databaseName, position, CatalogEntity.getEnumNameOrNull( type ), length, precision, nullable, CatalogEntity.getEnumNameOrNull( encoding ), CatalogEntity.getEnumNameOrNull( collation ), forceDefault };
    }


    @RequiredArgsConstructor
    public class PrimitiveCatalogColumn {
        public final String name;
        public final String table;
        public final String schema;
        public final String database;
        public final int position;
        public final String type;
        public final Integer length;
        public final Integer precision;
        public final boolean nullable;
        public final String encoding;
        public final String collation;
        public final boolean forceDefault;
    }
}
