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

package org.polypheny.db.sql.language;


import java.sql.Connection;
import java.sql.DatabaseMetaData;


/**
 * Creates a <code>SqlDialect</code> appropriate for a given database metadata object.
 */
public interface SqlDialectFactory {

    /**
     * Creates a <code>SqlDialect</code> from a DatabaseMetaData.
     * <p>
     * Does not maintain a reference to the DatabaseMetaData -- or, more* importantly, to its {@link Connection} -- after this call has returned.
     *
     * @param databaseMetaData used to determine which dialect of SQL to generate
     * @throws RuntimeException if there was an error creating the dialect
     */
    SqlDialect create( DatabaseMetaData databaseMetaData );

}
