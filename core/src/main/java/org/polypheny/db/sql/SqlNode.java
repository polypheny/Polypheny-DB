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
 *
 * This file incorporates code covered by the following terms:
 *
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to you under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.polypheny.db.sql;


import java.util.Collection;
import java.util.List;
import java.util.Objects;
import java.util.Set;
import org.polypheny.db.core.Node;
import org.polypheny.db.sql.dialect.AnsiSqlDialect;
import org.polypheny.db.core.ParserPos;
import org.polypheny.db.sql.pretty.SqlPrettyWriter;
import org.polypheny.db.sql.util.SqlString;
import org.polypheny.db.sql.util.SqlVisitor;
import org.polypheny.db.sql.validate.SqlMoniker;
import org.polypheny.db.sql.validate.SqlMonotonicity;
import org.polypheny.db.sql.validate.SqlValidator;
import org.polypheny.db.sql.validate.SqlValidatorScope;
import org.polypheny.db.util.Litmus;
import org.polypheny.db.util.Util;


/**
 * A <code>SqlNode</code> is a SQL parse tree.
 *
 * It may be an {@link SqlOperator operator}, {@link SqlLiteral literal}, {@link SqlIdentifier identifier}, and so forth.
 */
public abstract class SqlNode implements Cloneable, Node {

    public static final SqlNode[] EMPTY_ARRAY = new SqlNode[0];


    protected final ParserPos pos;


    /**
     * Creates a node.
     *
     * @param pos Parser position, must not be null.
     */
    SqlNode( ParserPos pos ) {
        this.pos = Objects.requireNonNull( pos );
    }


    /**
     * @deprecated Please use {@link #clone(SqlNode)}; this method brings along too much baggage from early versions of Java
     */
    @Override
    @Deprecated
    @SuppressWarnings("MethodDoesntCallSuperMethod")
    public Object clone() {
        return clone( getPos() );
    }


    /**
     * Creates a copy of a SqlNode.
     */
    public static <E extends SqlNode> E clone( E e ) {
        //noinspection unchecked
        return (E) e.clone( e.pos );
    }


    /**
     * Clones a SqlNode with a different position.
     */
    public abstract SqlNode clone( ParserPos pos );


    /**
     * Returns the type of node this is, or {@link SqlKind#OTHER} if it's nothing special.
     *
     * @return a {@link SqlKind} value, never null
     * @see #isA
     */
    public SqlKind getKind() {
        return SqlKind.OTHER;
    }


    /**
     * Returns whether this node is a member of an aggregate category.
     *
     * For example, {@code node.isA(SqlKind.QUERY)} returns {@code true} if the node is a SELECT, INSERT, UPDATE etc.
     *
     * This method is shorthand: {@code node.isA(category)} is always equivalent to {@code node.getKind().belongsTo(category)}.
     *
     * @param category Category
     * @return Whether this node belongs to the given category.
     */
    public final boolean isA( Set<SqlKind> category ) {
        return getKind().belongsTo( category );
    }


    public String toString() {
        return toSqlString( null ).getSql();
    }


    /**
     * Returns the SQL text of the tree of which this <code>SqlNode</code> is the root.
     *
     * @param dialect Dialect
     * @param forceParens wraps all expressions in parentheses; good for parse test, but false by default.
     *
     * Typical return values are:
     * <ul>
     * <li>'It''s a bird!'</li>
     * <li>NULL</li>
     * <li>12.3</li>
     * <li>DATE '1969-04-29'</li>
     * </ul>
     */
    public SqlString toSqlString( SqlDialect dialect, boolean forceParens ) {
        if ( dialect == null ) {
            //if you change this default dialect, you should consider changing the SqlParserTest.nullDialect as well!
            dialect = AnsiSqlDialect.DEFAULT;
        }
        SqlPrettyWriter writer = new SqlPrettyWriter( dialect );
        writer.setAlwaysUseParentheses( forceParens );
        writer.setSelectListItemsOnSeparateLines( false );
        writer.setIndentation( 0 );
        unparse( writer, 0, 0 );
        return writer.toSqlString();
    }


    public SqlString toSqlString( SqlDialect dialect ) {
        return toSqlString( dialect, false );
    }


    /**
     * Writes a SQL representation of this node to a writer.
     *
     * The <code>leftPrec</code> and <code>rightPrec</code> parameters give us enough context to decide whether we need to enclose the expression in parentheses. For example, we need parentheses around "2 + 3" if preceded by "5 *". This is because the precedence of the "*" operator is greater than the precedence of the "+" operator.
     *
     * The algorithm handles left- and right-associative operators by giving them slightly different left- and right-precedence.
     *
     * If {@link SqlWriter#isAlwaysUseParentheses()} is true, we use parentheses even when they are not required by the precedence rules.
     *
     * For the details of this algorithm, see {@link SqlCall#unparse}.
     *
     * @param writer Target writer
     * @param leftPrec The precedence of the {@link SqlNode} immediately preceding this node in a depth-first scan of the parse tree
     * @param rightPrec The precedence of the {@link SqlNode} immediately
     */
    public abstract void unparse( SqlWriter writer, int leftPrec, int rightPrec );


    @Override
    public ParserPos getPos() {
        return pos;
    }


    /**
     * Validates this node.
     *
     * The typical implementation of this method will make a callback to the validator appropriate to the node type and context. The validator has methods such as {@link SqlValidator#validateLiteral} for these purposes.
     *
     * @param scope Validator
     */
    public abstract void validate( SqlValidator validator, SqlValidatorScope scope );


    /**
     * Lists all the valid alternatives for this node if the parse position of the node matches that of pos. Only implemented now for SqlCall and SqlOperator.
     *
     * @param validator Validator
     * @param scope Validation scope
     * @param pos SqlParserPos indicating the cursor position at which completion hints are requested for
     * @param hintList list of valid options
     */
    public void findValidOptions( SqlValidator validator, SqlValidatorScope scope, ParserPos pos, Collection<SqlMoniker> hintList ) {
        // no valid options
    }


    /**
     * Validates this node in an expression context.
     *
     * Usually, this method does much the same as {@link #validate}, but a {@link SqlIdentifier} can occur in expression and non-expression contexts.
     */
    public void validateExpr( SqlValidator validator, SqlValidatorScope scope ) {
        validate( validator, scope );
        Util.discard( validator.deriveType( scope, this ) );
    }


    /**
     * Accepts a generic visitor.
     *
     * Implementations of this method in subtypes simply call the appropriate <code>visit</code> method on the {@link SqlVisitor visitor object}.
     *
     * The type parameter <code>R</code> must be consistent with the type parameter of the visitor.
     */
    public abstract <R> R accept( SqlVisitor<R> visitor );

    /**
     * Returns whether this node is structurally equivalent to another node.
     * Some examples:
     *
     * <ul>
     * <li>1 + 2 is structurally equivalent to 1 + 2</li>
     * <li>1 + 2 + 3 is structurally equivalent to (1 + 2) + 3, but not to 1 + (2 + 3), because the '+' operator is left-associative</li>
     * </ul>
     */
    public abstract boolean equalsDeep( SqlNode node, Litmus litmus );


    /**
     * Returns whether two nodes are equal (using {@link #equalsDeep(SqlNode, Litmus)}) or are both null.
     *
     * @param node1 First expression
     * @param node2 Second expression
     * @param litmus What to do if an error is detected (expressions are not equal)
     */
    public static boolean equalDeep( SqlNode node1, SqlNode node2, Litmus litmus ) {
        if ( node1 == null ) {
            return node2 == null;
        } else if ( node2 == null ) {
            return false;
        } else {
            return node1.equalsDeep( node2, litmus );
        }
    }


    /**
     * Returns whether expression is always ascending, descending or constant.
     * This property is useful because it allows to safely aggregate infinite streams of values.
     *
     * The default implementation returns {@link SqlMonotonicity#NOT_MONOTONIC}.
     *
     * @param scope Scope
     */
    public SqlMonotonicity getMonotonicity( SqlValidatorScope scope ) {
        return SqlMonotonicity.NOT_MONOTONIC;
    }


    /**
     * Returns whether two lists of operands are equal.
     */
    public static boolean equalDeep( List<SqlNode> operands0, List<SqlNode> operands1, Litmus litmus ) {
        if ( operands0.size() != operands1.size() ) {
            return litmus.fail( null );
        }
        for ( int i = 0; i < operands0.size(); i++ ) {
            if ( !SqlNode.equalDeep( operands0.get( i ), operands1.get( i ), litmus ) ) {
                return litmus.fail( null );
            }
        }
        return litmus.succeed();
    }
}

