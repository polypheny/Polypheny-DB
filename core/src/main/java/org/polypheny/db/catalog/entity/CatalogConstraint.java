/*
 * Copyright 2019-2021 The Polypheny Project
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
import lombok.NonNull;
import org.polypheny.db.catalog.logistic.ConstraintType;


@EqualsAndHashCode
public class CatalogConstraint implements Serializable {

    public final long id;
    public final long keyId;
    public final ConstraintType type;
    public final String name;

    public final CatalogKey key;


    public CatalogConstraint(
            final long id,
            final long keyId,
            @NonNull final ConstraintType constraintType,
            final String name,
            final CatalogKey key ) {
        this.id = id;
        this.keyId = keyId;
        this.type = constraintType;
        this.name = name;
        this.key = key;
    }


}
