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


import org.polypheny.db.algebra.type.AlgDataTypeFactory;
import org.polypheny.db.rex.RexBuilder;
import org.polypheny.db.rex.RexNode;
import org.polypheny.db.rex.RexRangeRef;
import org.polypheny.db.sql.language.SqlCall;
import org.polypheny.db.sql.language.SqlLiteral;
import org.polypheny.db.sql.language.SqlNode;
import org.polypheny.db.sql.language.SqlOperatorBinding;
import org.polypheny.db.sql.language.SqlSelect;
import org.polypheny.db.sql.language.validate.SqlValidator;
import org.polypheny.db.util.InitializerExpressionFactory;


/**
 * Contains the context necessary for a {@link SqlRexConvertlet} to convert a {@link SqlNode} expression into a {@link RexNode}.
 */
public interface SqlRexContext {

    /**
     * Converts an expression from {@link SqlNode} to {@link RexNode} format.
     *
     * @param expr Expression to translate
     * @return Converted expression
     */
    RexNode convertExpression( SqlNode expr );

    /**
     * If the operator call occurs in an aggregate query, returns the number of columns in the GROUP BY clause. For example, for "SELECT count(*) FROM emp GROUP BY deptno, gender", returns 2.
     * If the operator call occurs in window aggregate query, then returns 1 if the window is guaranteed to be non-empty, or 0 if the window might be empty.
     *
     * Returns 0 if the query is implicitly "GROUP BY ()" because of an aggregate expression. For example, "SELECT sum(sal) FROM emp".
     *
     * Returns -1 if the query is not an aggregate query.
     *
     * @return 0 if the query is implicitly GROUP BY (), -1 if the query is not and aggregate query
     * @see SqlOperatorBinding#getGroupCount()
     */
    int getGroupCount();

    /**
     * Returns the {@link RexBuilder} to use to create {@link RexNode} objects.
     */
    RexBuilder getRexBuilder();

    /**
     * Returns the expression used to access a given IN or EXISTS {@link SqlSelect sub-query}.
     *
     * @param call IN or EXISTS expression
     * @return Expression used to access current row of sub-query
     */
    RexRangeRef getSubQueryExpr( SqlCall call );

    /**
     * Returns the type factory.
     */
    AlgDataTypeFactory getTypeFactory();

    /**
     * Returns the factory which supplies default values for INSERT, UPDATE, and NEW.
     */
    InitializerExpressionFactory getInitializerExpressionFactory();

    /**
     * Returns the validator.
     */
    SqlValidator getValidator();

    /**
     * Converts a literal.
     */
    RexNode convertLiteral( SqlLiteral literal );

}
