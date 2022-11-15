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

package org.polypheny.db.sql.language.ddl;


import org.polypheny.db.algebra.constant.Kind;
import org.polypheny.db.languages.ParserPos;
import org.polypheny.db.sql.language.SqlAlter;
import org.polypheny.db.sql.language.SqlOperator;
import org.polypheny.db.sql.language.SqlSpecialOperator;


/**
 * Parse tree for {@code ALTER SCHEMA} statement.
 */
public abstract class SqlAlterSchema extends SqlAlter {


    private static final SqlOperator OPERATOR = new SqlSpecialOperator( "ALTER SCHEMA", Kind.ALTER_SCHEMA );


    /**
     * Creates a SqlAlterSchema.
     */
    public SqlAlterSchema( ParserPos pos ) {
        super( OPERATOR, pos );
    }

}
