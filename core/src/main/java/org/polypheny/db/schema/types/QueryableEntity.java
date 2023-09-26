/*
 * Copyright 2019-2023 The Polypheny Project
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

package org.polypheny.db.schema.types;

import java.lang.reflect.Type;
import org.apache.calcite.linq4j.Queryable;
import org.polypheny.db.adapter.DataContext;
import org.polypheny.db.catalog.snapshot.Snapshot;
import org.polypheny.db.type.entity.PolyValue;

public interface QueryableEntity extends Typed {

    /**
     * Converts this table into a {@link Queryable}.
     */
    Queryable<PolyValue[]> asQueryable( DataContext dataContext, Snapshot snapshot );

    default Type getElementType() {
        return PolyValue[].class;
    }

}
