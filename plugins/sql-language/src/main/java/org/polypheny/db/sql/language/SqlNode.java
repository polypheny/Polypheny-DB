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


import java.util.Collection;
import java.util.Objects;
import java.util.Set;
import org.jetbrains.annotations.Nullable;
import org.polypheny.db.algebra.constant.Kind;
import org.polypheny.db.algebra.constant.Monotonicity;
import org.polypheny.db.languages.ParserPos;
import org.polypheny.db.languages.QueryLanguage;
import org.polypheny.db.nodes.Node;
import org.polypheny.db.sql.language.dialect.AnsiSqlDialect;
import org.polypheny.db.sql.language.pretty.SqlPrettyWriter;
import org.polypheny.db.sql.language.util.SqlString;
import org.polypheny.db.sql.language.validate.SqlValidator;
import org.polypheny.db.sql.language.validate.SqlValidatorScope;
import org.polypheny.db.util.Moniker;
import org.polypheny.db.util.Util;


/**
 * A <code>SqlNode</code> is a SQL parse tree.
 *
 * It may be an {@link SqlOperator operator}, {@link SqlLiteral literal}, {@link SqlIdentifier identifier}, and so forth.
 */
public abstract class SqlNode implements Node {

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
     * @deprecated Please use {@link #clone(Node)}; this method brings along too much baggage from early versions of Java
     */
    @Override
    @Deprecated
    @SuppressWarnings("MethodDoesntCallSuperMethod")
    public Object clone() {
        return clone( getPos() );
    }


    @Override
    public @Nullable String getEntity() {
        return null;
    }


    @Override
    public QueryLanguage getLanguage() {
        return QueryLanguage.from( "sql" );
    }


    /**
     * Returns the type of node this is, or {@link Kind#OTHER} if it's nothing special.
     *
     * @return a {@link Kind} value, never null
     * @see #isA
     */
    @Override
    public Kind getKind() {
        return Kind.OTHER;
    }


    /**
     * Returns whether this node is a member of an aggregate category.
     *
     * For example, {@code node.isA(Kind.QUERY)} returns {@code true} if the node is a SELECT, INSERT, UPDATE etc.
     *
     * This method is shorthand: {@code node.isA(category)} is always equivalent to {@code node.getKind().belongsTo(category)}.
     *
     * @param category Category
     * @return Whether this node belongs to the given category.
     */
    @Override
    public final boolean isA( Set<Kind> category ) {
        return getKind().belongsTo( category );
    }


    @Override
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
     * @param leftPrec The precedence of the {@link SqlNode} immediately preceding this node in a depth-first relScan of the parse tree
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
    public void findValidOptions( SqlValidator validator, SqlValidatorScope scope, ParserPos pos, Collection<Moniker> hintList ) {
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
     * Returns whether expression is always ascending, descending or constant.
     * This property is useful because it allows to safely aggregate infinite streams of values.
     *
     * The default implementation returns {@link Monotonicity#NOT_MONOTONIC}.
     *
     * @param scope Scope
     */
    public Monotonicity getMonotonicity( SqlValidatorScope scope ) {
        return Monotonicity.NOT_MONOTONIC;
    }


}

