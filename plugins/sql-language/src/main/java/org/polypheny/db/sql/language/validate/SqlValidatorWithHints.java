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

package org.polypheny.db.sql.language.validate;


import java.util.List;
import org.polypheny.db.languages.ParserPos;
import org.polypheny.db.sql.language.SqlIdentifier;
import org.polypheny.db.sql.language.SqlNode;
import org.polypheny.db.util.Moniker;


/**
 * Extends {@link SqlValidator} to allow discovery of useful data such as fully qualified names of sql objects, alternative valid sql objects that can be
 * used in the SQL statement (dubbed as hints)
 */
public interface SqlValidatorWithHints extends SqlValidator {

    /**
     * Looks up completion hints for a syntactically correct SQL statement that has been parsed into an expression tree. (Note this should be called
     * after {@link #validateSql(SqlNode)}.
     *
     * @param topNode top of expression tree in which to lookup completion hints
     * @param pos indicates the position in the sql statement we want to get completion hints for. For example, "select a.ename, b.deptno from sales.emp a join sales.dept b "on a.deptno=b.deptno where empno=1"; setting pos to 'Line 1, Column 17' returns all the possible column names that can be selected from sales.dept table setting pos to 'Line 1, Column 31' returns all the possible table names in 'sales' schema
     * @return A list of {@link Moniker} (sql identifiers) that can fill in at the indicated position
     */
    List<Moniker> lookupHints( SqlNode topNode, ParserPos pos );

    /**
     * Looks up the fully qualified name for a {@link SqlIdentifier} at a given Parser Position in a parsed expression tree Note: call this only after
     * {@link #validateSql} has been called.
     *
     * @param topNode top of expression tree in which to lookup the qualified name for the SqlIdentifier
     * @param pos indicates the position of the {@link SqlIdentifier} in the SQL statement we want to get the qualified name for
     * @return a string of the fully qualified name of the {@link SqlIdentifier} if the Parser position represents a valid {@link SqlIdentifier}. Else return an empty string
     */
    Moniker lookupQualifiedName( SqlNode topNode, ParserPos pos );

}

