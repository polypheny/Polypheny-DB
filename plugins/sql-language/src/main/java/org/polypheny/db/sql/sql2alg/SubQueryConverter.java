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

package org.polypheny.db.sql.sql2alg;


import org.polypheny.db.rex.RexNode;
import org.polypheny.db.sql.language.SqlCall;


/**
 * SubQueryConverter provides the interface for classes that convert sub-queries into equivalent expressions.
 */
public interface SubQueryConverter {

    /**
     * @return Whether the sub-query can be converted
     */
    boolean canConvertSubQuery();

    /**
     * Converts the sub-query to an equivalent expression.
     *
     * @param subQuery the SqlNode tree corresponding to a sub-query
     * @param parentConverter sqlToRelConverter of the parent query
     * @param isExists whether the sub-query is part of an EXISTS expression
     * @param isExplain whether the sub-query is part of an EXPLAIN PLAN statement
     * @return the equivalent expression or null if the sub-query couldn't be converted
     */
    RexNode convertSubQuery( SqlCall subQuery, SqlToAlgConverter parentConverter, boolean isExists, boolean isExplain );

}

