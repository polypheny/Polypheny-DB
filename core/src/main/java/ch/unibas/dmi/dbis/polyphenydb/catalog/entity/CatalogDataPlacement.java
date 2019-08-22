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


import java.io.Serializable;
import lombok.NonNull;
import lombok.RequiredArgsConstructor;


public class CatalogDataPlacement implements CatalogEntity {

    private static final long serialVersionUID = 4754069156177607149L;

    public final long tableId;
    public final String tableName;
    public final int storeId;
    public final String storeUniqueName;


    public CatalogDataPlacement( final long tableId, @NonNull final String tableName, final int storeId, @NonNull final String storeUniqueName ) {
        this.tableId = tableId;
        this.tableName = tableName;
        this.storeId = storeId;
        this.storeUniqueName = storeUniqueName;
    }


    // Used for creating ResultSets
    @Override
    public Serializable[] getParameterArray() {
        return new Serializable[]{ tableName, storeUniqueName };
    }


    @RequiredArgsConstructor
    public static class PrimitiveCatalogUser {

        public final String table;
        public final String store;
    }
}
