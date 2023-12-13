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

package org.polypheny.db.sql.language.ddl;

import lombok.Getter;
import org.polypheny.db.languages.ParserPos;
import org.polypheny.db.nodes.Operator;
import org.polypheny.db.sql.language.SqlIdentifier;
import org.polypheny.db.sql.language.SqlNodeList;

@Getter
public class SqlForeignKeyConstraint extends SqlKeyConstraint {

    private final SqlIdentifier referencedTable;
    private final SqlIdentifier referencedColumn;


    /**
     * Creates a SqlKeyConstraint.
     *
     * @param pos
     * @param name
     * @param columnList
     * @param referencedTable
     * @param referencedColumn
     */
    SqlForeignKeyConstraint( ParserPos pos, SqlIdentifier name, SqlNodeList columnList, SqlIdentifier referencedTable, SqlIdentifier referencedColumn ) {
        super( pos, name, columnList );
        this.referencedTable = referencedTable;
        this.referencedColumn = referencedColumn;
    }


    @Override
    public Operator getOperator() {
        return FOREIGN;
    }

}
