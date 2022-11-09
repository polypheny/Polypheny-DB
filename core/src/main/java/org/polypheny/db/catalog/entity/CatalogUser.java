/*
 * Copyright 2019-2022 The Polypheny Project
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

package org.polypheny.db.catalog.entity;


import java.io.Serializable;
import lombok.EqualsAndHashCode;
import lombok.RequiredArgsConstructor;


@EqualsAndHashCode
public final class CatalogUser implements CatalogObject, Comparable<CatalogUser> {

    private static final long serialVersionUID = 5022567585804699491L;

    public final int id;
    public final String name;
    public final String password;


    public CatalogUser( final int id, final String name, final String password ) {
        this.id = id;
        this.name = name;
        this.password = password;
    }


    // Used for creating ResultSets
    @Override
    public Serializable[] getParameterArray() {
        return new Serializable[]{ name };
    }


    @Override
    public int compareTo( CatalogUser o ) {
        if ( o != null ) {
            return this.id - o.id;
        }
        return -1;
    }


    @RequiredArgsConstructor
    public static class PrimitiveCatalogUser {

        public final String name;

    }

}
