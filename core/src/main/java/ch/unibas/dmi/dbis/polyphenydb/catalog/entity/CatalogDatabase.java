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


import ch.unibas.dmi.dbis.polyphenydb.catalog.Catalog.Collation;
import ch.unibas.dmi.dbis.polyphenydb.catalog.Catalog.Encoding;
import lombok.EqualsAndHashCode;
import lombok.NonNull;
import lombok.RequiredArgsConstructor;


/**
 *
 */
@EqualsAndHashCode
public final class CatalogDatabase implements CatalogEntity {

    private static final long serialVersionUID = -9134468391960083988L;

    public final String name;
    public final String owner;
    public final Encoding encoding;
    public final Collation collation;
    public final int connectionLimit;


    public CatalogDatabase( @NonNull final String name, final String owner, final Encoding encoding, final Collation collation, final int connectionLimit ) {
        this.name = name;
        this.owner = owner;
        this.encoding = encoding;
        this.collation = collation;
        this.connectionLimit = connectionLimit;
    }


    // Used for creating ResultSets
    @Override
    public Object[] getParameterArray() {
        return new Object[]{ name, owner, encoding.name(), collation.name(), connectionLimit };
    }


    @RequiredArgsConstructor
    public class PrimitiveCatalogDatabase {

        public final String name;
        public final String owner;
        public final String encoding;
        public final String collation;
        public final int connectionLimit;
    }
}
