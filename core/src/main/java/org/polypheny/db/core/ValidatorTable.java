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

package org.polypheny.db.core;


import java.util.List;
import org.polypheny.db.rel.type.RelDataType;
import org.polypheny.db.schema.Wrapper;


/**
 * Supplies a {@link Validator} with the metadata for a table.
 *
 * #@see ValidatorCatalogReader
 */
public interface ValidatorTable extends Wrapper {


    RelDataType getRowType();

    List<String> getQualifiedName();

    /**
     * Returns whether a given column is monotonic.
     */
    Monotonicity getMonotonicity( String columnName );

    /**
     * Returns the access type of the table
     */
    AccessType getAllowedAccess();

    boolean supportsModality( Modality modality );

}

