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

package org.polypheny.db.languages.sql;


import java.util.AbstractList;
import java.util.List;
import org.polypheny.db.languages.sql.validate.SqlMonotonicity;
import org.polypheny.db.languages.sql.validate.SqlValidatorException;
import org.polypheny.db.rel.type.RelDataType;
import org.polypheny.db.rel.type.RelDataTypeFactory;
import org.polypheny.db.runtime.PolyphenyDbException;
import org.polypheny.db.runtime.Resources.ExInst;
import org.polypheny.db.util.NlsString;


/**
 * <code>SqlOperatorBinding</code> represents the binding of an {@link SqlOperator} to actual operands, along with any additional information required to validate those operands if needed.
 */
public abstract class SqlOperatorBinding {

    protected final RelDataTypeFactory typeFactory;
    private final SqlOperator sqlOperator;


    /**
     * Creates a SqlOperatorBinding.
     *
     * @param typeFactory Type factory
     * @param sqlOperator Operator which is subject of this call
     */
    protected SqlOperatorBinding( RelDataTypeFactory typeFactory, SqlOperator sqlOperator ) {
        this.typeFactory = typeFactory;
        this.sqlOperator = sqlOperator;
    }


    /**
     * If the operator call occurs in an aggregate query, returns the number of columns in the GROUP BY clause. For example, for "SELECT count(*) FROM emp GROUP BY deptno, gender", returns 2.
     *
     * Returns 0 if the query is implicitly "GROUP BY ()" because of an aggregate expression. For example, "SELECT sum(sal) FROM emp".
     *
     * Returns -1 if the query is not an aggregate query.
     */
    public int getGroupCount() {
        return -1;
    }


    /**
     * Returns whether the operator is an aggregate function with a filter.
     */
    public boolean hasFilter() {
        return false;
    }


    /**
     * @return bound operator
     */
    public SqlOperator getOperator() {
        return sqlOperator;
    }


    /**
     * @return factory for type creation
     */
    public RelDataTypeFactory getTypeFactory() {
        return typeFactory;
    }


    /**
     * Gets the value of a literal operand.
     *
     * Cases:
     * <ul>
     * <li>If the operand is not a literal, the value is null.</li>
     * <li>If the operand is a string literal, the value will be of type {@link NlsString}.</li>
     * <li>If the operand is a numeric literal, the value will be of type {@link java.math.BigDecimal}.</li>
     * <li>If the operand is an interval qualifier, the value will be of type {@link SqlIntervalQualifier}</li>
     * <li>Otherwise the type is undefined, and the value may be null.</li>
     * </ul>
     *
     * @param ordinal zero-based ordinal of operand of interest
     * @param clazz Desired valued type
     * @return value of operand
     */
    public <T> T getOperandLiteralValue( int ordinal, Class<T> clazz ) {
        throw new UnsupportedOperationException();
    }


    /**
     * Determines whether a bound operand is NULL.
     *
     * This is only relevant for SQL validation.
     *
     * @param ordinal zero-based ordinal of operand of interest
     * @param allowCast whether to regard CAST(constant) as a constant
     * @return whether operand is null; false for everything except SQL validation
     */
    public boolean isOperandNull( int ordinal, boolean allowCast ) {
        throw new UnsupportedOperationException();
    }


    /**
     * Determines whether an operand is a literal.
     *
     * @param ordinal zero-based ordinal of operand of interest
     * @param allowCast whether to regard CAST(literal) as a literal
     * @return whether operand is literal
     */
    public boolean isOperandLiteral( int ordinal, boolean allowCast ) {
        throw new UnsupportedOperationException();
    }


    /**
     * @return the number of bound operands
     */
    public abstract int getOperandCount();

    /**
     * Gets the type of a bound operand.
     *
     * @param ordinal zero-based ordinal of operand of interest
     * @return bound operand type
     */
    public abstract RelDataType getOperandType( int ordinal );


    /**
     * Gets the monotonicity of a bound operand.
     *
     * @param ordinal zero-based ordinal of operand of interest
     * @return monotonicity of operand
     */
    public SqlMonotonicity getOperandMonotonicity( int ordinal ) {
        return SqlMonotonicity.NOT_MONOTONIC;
    }


    /**
     * Collects the types of the bound operands into a list.
     *
     * @return collected list
     */
    public List<RelDataType> collectOperandTypes() {
        return new AbstractList<RelDataType>() {
            @Override
            public RelDataType get( int index ) {
                return getOperandType( index );
            }


            @Override
            public int size() {
                return getOperandCount();
            }
        };
    }


    /**
     * Returns the rowtype of the <code>ordinal</code>th operand, which is a cursor.
     *
     * This is only implemented for {@link SqlCallBinding}.
     *
     * @param ordinal Ordinal of the operand
     * @return Rowtype of the query underlying the cursor
     */
    public RelDataType getCursorOperand( int ordinal ) {
        throw new UnsupportedOperationException();
    }


    /**
     * Retrieves information about a column list parameter.
     *
     * @param ordinal ordinal position of the column list parameter
     * @param paramName name of the column list parameter
     * @param columnList returns a list of the column names that are referenced in the column list parameter
     * @return the name of the parent cursor referenced by the column list parameter if it is a column list parameter; otherwise, null is returned
     */
    public String getColumnListParamInfo( int ordinal, String paramName, List<String> columnList ) {
        throw new UnsupportedOperationException();
    }


    /**
     * Wraps a validation error with context appropriate to this operator call.
     *
     * @param e Validation error, not null
     * @return Error wrapped, if possible, with positional information
     */
    public abstract PolyphenyDbException newError( ExInst<SqlValidatorException> e );
}

