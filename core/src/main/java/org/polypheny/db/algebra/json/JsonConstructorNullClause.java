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

package org.polypheny.db.algebra.json;


/**
 * Indicating that how do Json constructors handle null
 */
public enum JsonConstructorNullClause {
    NULL_ON_NULL( "NULL ON NULL" ),
    ABSENT_ON_NULL( "ABSENT ON NULL" );

    public final String sql;


    JsonConstructorNullClause( String sql ) {
        this.sql = sql;
    }
}

