/*
 * This file is based on code taken from the Apache Calcite project, which was released under the Apache License.
 * The changes are released under the MIT license.
 *
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
 *
 *
 * The MIT License (MIT)
 *
 * Copyright (c) 2019 Databases and Information Systems Research Group, University of Basel, Switzerland
 *
 * Permission is hereby granted, free of charge, to any person obtaining a
 * copy of this software and associated documentation files (the "Software"), to deal
 * in the Software without restriction, including without limitation the rights
 * to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
 * copies of the Software, and to permit persons to whom the Software is
 * furnished to do so, subject to the following conditions:
 *
 * The above copyright notice and this permission notice shall be included in all
 * copies or substantial portions of the Software.
 *
 * THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
 * IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
 * FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
 * AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
 * LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
 * OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
 * SOFTWARE.
 */

package ch.unibas.dmi.dbis.polyphenydb.rex;


import ch.unibas.dmi.dbis.polyphenydb.rel.type.RelDataType;
import ch.unibas.dmi.dbis.polyphenydb.rel.type.RelDataTypeField;
import ch.unibas.dmi.dbis.polyphenydb.sql.SqlKind;


/**
 * Access to a field of a row-expression.
 *
 * You might expect to use a <code>RexFieldAccess</code> to access columns of relational tables, for example, the expression <code>emp.empno</code> in the query
 *
 * <blockquote>
 * <pre>SELECT emp.empno FROM emp</pre>
 * </blockquote>
 *
 * but there is a specialized expression {@link RexInputRef} for this purpose. So in practice, <code>RexFieldAccess</code> is usually used to access fields of correlating variables,
 * for example the expression <code>emp.deptno</code> in
 *
 * <blockquote>
 * <pre>
 * SELECT ename
 * FROM dept
 * WHERE EXISTS (
 *     SELECT NULL
 *     FROM emp
 *     WHERE emp.deptno = dept.deptno
 *     AND gender = 'F')
 * </pre>
 * </blockquote>
 */
public class RexFieldAccess extends RexNode {

    private final RexNode expr;
    private final RelDataTypeField field;


    RexFieldAccess( RexNode expr, RelDataTypeField field ) {
        this.expr = expr;
        this.field = field;
        this.digest = expr + "." + field.getName();
        assert expr.getType().getFieldList().get( field.getIndex() ) == field;
    }


    public RelDataTypeField getField() {
        return field;
    }


    @Override
    public RelDataType getType() {
        return field.getType();
    }


    @Override
    public SqlKind getKind() {
        return SqlKind.FIELD_ACCESS;
    }


    @Override
    public <R> R accept( RexVisitor<R> visitor ) {
        return visitor.visitFieldAccess( this );
    }


    @Override
    public <R, P> R accept( RexBiVisitor<R, P> visitor, P arg ) {
        return visitor.visitFieldAccess( this, arg );
    }


    /**
     * Returns the expression whose field is being accessed.
     */
    public RexNode getReferenceExpr() {
        return expr;
    }


    @Override
    public boolean equals( Object o ) {
        if ( this == o ) {
            return true;
        }
        if ( o == null || getClass() != o.getClass() ) {
            return false;
        }

        RexFieldAccess that = (RexFieldAccess) o;

        return field.equals( that.field ) && expr.equals( that.expr );
    }


    @Override
    public int hashCode() {
        int result = expr.hashCode();
        result = 31 * result + field.hashCode();
        return result;
    }
}

