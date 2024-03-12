
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

/**
 * Provides a SQL parser and object model.
 *
 * This package, and the dependent <code>org.polypheny.db.sql.parser</code> package, are independent of the other Polypheny-DB packages,
 * so may be used standalone.
 *
 * <h2>Parser</h2>
 *
 * {@link org.polypheny.db.sql.language.parser.SqlParser} parses a SQL string to a parse tree. It only performs the most basic syntactic validation.
 *
 * <h2>Object model</h2>
 *
 * Every node in the parse tree is a {@link org.polypheny.db.sql.language.SqlNode}. Sub-types are:
 *
 * <ul>
 * <li>{@link org.polypheny.db.sql.language.SqlLiteral} represents a boolean, numeric, string, or date constant, or the value <code>NULL</code>.</li>
 * <li>{@link org.polypheny.db.sql.language.SqlIdentifier} represents an identifier, such as <code> EMPNO</code> or <code>emp.deptno</code>.</li>
 * <li>{@link org.polypheny.db.sql.language.SqlCall} is a call to an operator or function.  By means of special operators, we can use this construct to represent virtually every non-leaf node in the tree. For example, a <code>select</code> statement is a call to the 'select' operator.</li>
 * <li>{@link org.polypheny.db.sql.language.SqlNodeList} is a list of nodes.</li>
 * </ul>
 *
 * A {@link org.polypheny.db.sql.language.SqlOperator} describes the behavior of a node in the tree, such as how to un-parse a
 * {@link org.polypheny.db.sql.language.SqlCall} into a SQL string.  It is important to note that operators are metadata, not data: there is only
 * one <code>SqlOperator</code> instance representing the '=' operator, even though there may be many calls to it.
 *
 * <code>SqlOperator</code> has several derived classes which make it easy to define new operators:
 * {@link org.polypheny.db.sql.language.SqlFunction},
 * {@link org.polypheny.db.sql.language.SqlBinaryOperator},
 * {@link org.polypheny.db.sql.language.SqlPrefixOperator},
 * {@link org.polypheny.db.sql.language.SqlPostfixOperator}.
 * And there are singleton classes for special syntactic constructs {@link org.polypheny.db.sql.language.SqlSelectOperator} and
 * {@link org.polypheny.db.sql.language.SqlJoin.SqlJoinOperator}. (These special operators even have their own sub-types of {@link org.polypheny.db.sql.language.SqlCall}:
 * {@link org.polypheny.db.sql.language.SqlSelect} and
 * {@link org.polypheny.db.sql.language.SqlJoin}.)
 *
 * A {@link org.polypheny.db.algebra.operators.OperatorTable} is a collection of operators. By supplying your own operator table, you can customize the
 * dialect of SQL without modifying the parser.
 *
 * <h2>Validation</h2>
 *
 * {@link org.polypheny.db.sql.language.validate.SqlValidator} checks that a tree of {@link org.polypheny.db.sql.language.SqlNode}s is semantically valid.
 * You supply a {@link org.polypheny.db.algebra.operators.OperatorTable} to describe the available functions and operators, and a
 *
 * <h2>Generating SQL</h2>
 *
 * A {@link org.polypheny.db.sql.language.SqlWriter} converts a tree of {@link org.polypheny.db.sql.language.SqlNode}s into a SQL string.
 * A {@link org.polypheny.db.sql.language.SqlDialect} defines how this happens.
 */

package org.polypheny.db.sql.language;

