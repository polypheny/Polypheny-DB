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

package ch.unibas.dmi.dbis.polyphenydb.sql.fun;


import ch.unibas.dmi.dbis.polyphenydb.rel.type.RelDataType;
import ch.unibas.dmi.dbis.polyphenydb.rel.type.RelDataTypeFactory;
import ch.unibas.dmi.dbis.polyphenydb.sql.SqlAggFunction;
import ch.unibas.dmi.dbis.polyphenydb.sql.SqlFunctionCategory;
import ch.unibas.dmi.dbis.polyphenydb.sql.SqlKind;
import ch.unibas.dmi.dbis.polyphenydb.sql.SqlSplittableAggFunction;
import ch.unibas.dmi.dbis.polyphenydb.sql.type.OperandTypes;
import ch.unibas.dmi.dbis.polyphenydb.sql.type.ReturnTypes;
import ch.unibas.dmi.dbis.polyphenydb.util.Optionality;
import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;
import java.util.List;


/**
 * Definition of the <code>MIN</code> and <code>MAX</code> aggregate functions, returning the returns the smallest/largest of the values which go into it.
 *
 * There are 3 forms:
 *
 * <dl>
 * <dt>sum(<em>primitive type</em>)</dt>
 * <dd>values are compared using '&lt;'</dd>
 *
 * <dt>sum({@link java.lang.Comparable})</dt>
 * <dd>values are compared using {@link java.lang.Comparable#compareTo}</dd>
 *
 * <dt>sum({@link java.util.Comparator}, {@link java.lang.Object})</dt>
 * <dd>the {@link java.util.Comparator#compare} method of the comparator is used to compare pairs of objects. The comparator is a startup argument, and must therefore be constant for the duration of the aggregation.</dd>
 * </dl>
 */
public class SqlMinMaxAggFunction extends SqlAggFunction {

    public static final int MINMAX_INVALID = -1;
    public static final int MINMAX_PRIMITIVE = 0;
    public static final int MINMAX_COMPARABLE = 1;
    public static final int MINMAX_COMPARATOR = 2;


    @Deprecated // to be removed before 2.0
    public final List<RelDataType> argTypes;
    private final int minMaxKind;


    /**
     * Creates a SqlMinMaxAggFunction.
     */
    public SqlMinMaxAggFunction( SqlKind kind ) {
        super(
                kind.name(),
                null,
                kind,
                ReturnTypes.ARG0_NULLABLE_IF_EMPTY,
                null,
                OperandTypes.COMPARABLE_ORDERED,
                SqlFunctionCategory.SYSTEM,
                false,
                false,
                Optionality.FORBIDDEN );
        this.argTypes = ImmutableList.of();
        this.minMaxKind = MINMAX_COMPARABLE;
        Preconditions.checkArgument( kind == SqlKind.MIN || kind == SqlKind.MAX );
    }


    @Deprecated // to be removed before 2.0
    public SqlMinMaxAggFunction( List<RelDataType> argTypes, boolean isMin, int minMaxKind ) {
        this( isMin ? SqlKind.MIN : SqlKind.MAX );
        assert argTypes.isEmpty();
        assert minMaxKind == MINMAX_COMPARABLE;
    }


    @Deprecated // to be removed before 2.0
    public boolean isMin() {
        return kind == SqlKind.MIN;
    }


    @Deprecated // to be removed before 2.0
    public int getMinMaxKind() {
        return minMaxKind;
    }


    @SuppressWarnings("deprecation")
    public List<RelDataType> getParameterTypes( RelDataTypeFactory typeFactory ) {
        switch ( minMaxKind ) {
            case MINMAX_PRIMITIVE:
            case MINMAX_COMPARABLE:
                return argTypes;
            case MINMAX_COMPARATOR:
                return argTypes.subList( 1, 2 );
            default:
                throw new AssertionError( "bad kind: " + minMaxKind );
        }
    }


    @SuppressWarnings("deprecation")
    public RelDataType getReturnType( RelDataTypeFactory typeFactory ) {
        switch ( minMaxKind ) {
            case MINMAX_PRIMITIVE:
            case MINMAX_COMPARABLE:
                return argTypes.get( 0 );
            case MINMAX_COMPARATOR:
                return argTypes.get( 1 );
            default:
                throw new AssertionError( "bad kind: " + minMaxKind );
        }
    }


    @Override
    public <T> T unwrap( Class<T> clazz ) {
        if ( clazz == SqlSplittableAggFunction.class ) {
            return clazz.cast( SqlSplittableAggFunction.SelfSplitter.INSTANCE );
        }
        return super.unwrap( clazz );
    }
}

