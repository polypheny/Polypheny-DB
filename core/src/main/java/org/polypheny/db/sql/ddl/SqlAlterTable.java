/*
 * Copyright 2019-2020 The Polypheny Project
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

package org.polypheny.db.sql.ddl;


import org.polypheny.db.catalog.Catalog.TableType;
import org.polypheny.db.catalog.entity.CatalogTable;
import org.polypheny.db.sql.SqlAlter;
import org.polypheny.db.sql.SqlKind;
import org.polypheny.db.sql.SqlOperator;
import org.polypheny.db.sql.SqlSpecialOperator;
import org.polypheny.db.sql.parser.SqlParserPos;


/**
 * Parse tree for {@code ALTER TABLE} statement.
 */
public abstract class SqlAlterTable extends SqlAlter {


    private static final SqlOperator OPERATOR = new SqlSpecialOperator( "ALTER TABLE", SqlKind.ALTER_TABLE );


    /**
     * Creates a SqlAlterTable.
     */
    public SqlAlterTable( SqlParserPos pos ) {
        super( OPERATOR, pos );
    }

}
