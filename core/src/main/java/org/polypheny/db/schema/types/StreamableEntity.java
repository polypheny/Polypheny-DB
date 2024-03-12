/*
 * Copyright 2019-2024 The Polypheny Project
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


import org.polypheny.db.algebra.stream.Delta;
import org.polypheny.db.schema.Entity;


/**
 * Entity that can be converted to a namespace.
 *
 * @see Delta
 */
public interface StreamableEntity extends Typed {

    /**
     * Returns an enumerator over the tuples in this entity. Each tuple is represented as an array of its field values.
     */
    Entity stream();

}

