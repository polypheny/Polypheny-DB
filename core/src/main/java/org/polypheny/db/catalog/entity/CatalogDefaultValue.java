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
import org.polypheny.db.type.PolyType;


@EqualsAndHashCode
public class CatalogDefaultValue implements Serializable {

    private static final long serialVersionUID = 6085682952587659184L;

    public final long columnId;
    public final PolyType type;
    public final String value;
    public final String functionName;


    public CatalogDefaultValue(
            final long columnId,
            @NonNull final PolyType type,
            final String value,
            final String functionName ) {
        this.columnId = columnId;
        this.type = type;
        this.value = value;
        this.functionName = functionName;
    }


}
