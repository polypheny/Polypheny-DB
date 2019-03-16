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
import java.util.Objects;


/**
 * Reference to a range of columns.
 *
 * This construct is used only during the process of translating a {@link ch.unibas.dmi.dbis.polyphenydb.sql.SqlNode SQL} tree to a {@link ch.unibas.dmi.dbis.polyphenydb.rel.RelNode rel}/{@link RexNode rex} tree.
 * <em>Regular {@link RexNode rex} trees do not contain this construct.</em>
 *
 * While translating a join of EMP(EMPNO, ENAME, DEPTNO) to DEPT(DEPTNO2, DNAME) we create <code>RexRangeRef(DeptType,3)</code> to represent the pair of columns (DEPTNO2, DNAME) which came from DEPT.
 * The type has 2 columns, and therefore the range represents columns {3, 4} of the input.
 *
 * Suppose we later create a reference to the DNAME field of this RexRangeRef; it will return a <code>{@link RexInputRef}(5,Integer)</code>, and the {@link ch.unibas.dmi.dbis.polyphenydb.rex.RexRangeRef} will disappear.
 */
public class RexRangeRef extends RexNode {

    private final RelDataType type;
    private final int offset;


    /**
     * Creates a range reference.
     *
     * @param rangeType Type of the record returned
     * @param offset Offset of the first column within the input record
     */
    RexRangeRef( RelDataType rangeType, int offset ) {
        this.type = rangeType;
        this.offset = offset;
    }


    public RelDataType getType() {
        return type;
    }


    public int getOffset() {
        return offset;
    }


    public <R> R accept( RexVisitor<R> visitor ) {
        return visitor.visitRangeRef( this );
    }


    public <R, P> R accept( RexBiVisitor<R, P> visitor, P arg ) {
        return visitor.visitRangeRef( this, arg );
    }


    @Override
    public boolean equals( Object obj ) {
        return this == obj
                || obj instanceof RexRangeRef
                && type.equals( ((RexRangeRef) obj).type )
                && offset == ((RexRangeRef) obj).offset;
    }


    @Override
    public int hashCode() {
        return Objects.hash( type, offset );
    }
}

