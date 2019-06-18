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
import ch.unibas.dmi.dbis.polyphenydb.catalog.Catalog.Collation;
import ch.unibas.dmi.dbis.polyphenydb.catalog.Catalog.Encoding;
import java.io.Serializable;
import lombok.EqualsAndHashCode;
import lombok.NonNull;
import lombok.RequiredArgsConstructor;


/**
 *
 */
@EqualsAndHashCode
public final class CatalogColumn implements CatalogEntity {

    private static final long serialVersionUID = 8175898370446385726L;

    public final String name;
    public final String table;
    public final String schema;
    public final String database;
    public final int position;
    public final PolySqlType type;
    public final Integer length;
    public final Integer precision;
    public final boolean nullable;
    public final Encoding encoding;
    public final Collation collation;
    public final Long autoincrementStartValue;
    public final Long autoincrementNextValue;
    public final Serializable defaultValue;
    public final boolean forceDefault;


    public CatalogColumn( @NonNull final String name, @NonNull final String table, @NonNull final String schema, @NonNull final String database, final int position, @NonNull final PolySqlType type, final Integer length, final Integer precision, final boolean nullable, final Encoding encoding, final Collation collation, final Long autoincrementStartValue, final Long autoincrementNextValue, final Serializable defaultValue, final boolean forceDefault ) {
        this.name = name;
        this.table = table;
        this.schema = schema;
        this.database = database;
        this.position = position;
        this.type = type;
        this.length = length;
        this.precision = precision;
        this.nullable = nullable;
        this.encoding = encoding;
        this.collation = collation;
        this.autoincrementStartValue = autoincrementStartValue;
        this.autoincrementNextValue = autoincrementNextValue;
        this.defaultValue = defaultValue;
        this.forceDefault = forceDefault;
    }


    @Override
    public Object[] getParameterArray() {
        String dv = null;
        if ( defaultValue != null ) {
            if ( defaultValue instanceof String ) {
                dv = (String) defaultValue;
            } // TODO: Improve this
        }
        return new Object[]{ name, table, schema, database, position, CatalogEntity.getEnumNameOrNull( type ), length, precision, nullable, CatalogEntity.getEnumNameOrNull( encoding ), CatalogEntity.getEnumNameOrNull( collation ), autoincrementStartValue, autoincrementNextValue, dv, forceDefault };
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
        public final Long autoincrementStartValue;
        public final Long autoincrementNextValue;
        public final String defaultValue;
        public final boolean forceDefault;
    }
}
