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



/**
 * Entity whose row type can be extended to include extra fields.
 * <p>
 * In some storage systems, especially those with "late namespace", there may exist fields that have values in the entity but
 * which are not declared in the entity namespace. However, a particular query may wish to reference these fields as if they were
 * defined in the namespace.
 * <p>
 * If the entity implements extended interfaces such as
 * {@link ScannableEntity},
 * {@link FilterableEntity} or
 * {@link ProjectableFilterableEntity}
 */
public interface ExtensibleEntity extends Typed {

    /**
     * Returns the starting offset of the first extended field, which may differ from the field count when the entity stores
     * metadata fields that are not counted in the row-type field count.
     */
    int getExtendedColumnOffset();

}

