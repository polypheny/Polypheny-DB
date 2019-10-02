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

package ch.unibas.dmi.dbis.polyphenydb.sql.advise;


import ch.unibas.dmi.dbis.polyphenydb.DataContext;
import ch.unibas.dmi.dbis.polyphenydb.adapter.enumerable.CallImplementor;
import ch.unibas.dmi.dbis.polyphenydb.adapter.enumerable.NullPolicy;
import ch.unibas.dmi.dbis.polyphenydb.adapter.enumerable.RexImpTable;
import ch.unibas.dmi.dbis.polyphenydb.rel.type.RelDataType;
import ch.unibas.dmi.dbis.polyphenydb.rel.type.RelDataTypeFactory;
import ch.unibas.dmi.dbis.polyphenydb.schema.FunctionParameter;
import ch.unibas.dmi.dbis.polyphenydb.schema.ImplementableFunction;
import ch.unibas.dmi.dbis.polyphenydb.schema.TableFunction;
import ch.unibas.dmi.dbis.polyphenydb.schema.impl.ReflectiveFunctionBase;
import ch.unibas.dmi.dbis.polyphenydb.sql.validate.SqlMoniker;
import ch.unibas.dmi.dbis.polyphenydb.util.BuiltInMethod;
import com.google.common.collect.Iterables;
import java.lang.reflect.Method;
import java.lang.reflect.Type;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import org.apache.calcite.linq4j.Enumerable;
import org.apache.calcite.linq4j.Linq4j;
import org.apache.calcite.linq4j.tree.Expression;
import org.apache.calcite.linq4j.tree.Expressions;
import org.apache.calcite.linq4j.tree.Types;


/**
 * Table function that returns completion hints for a given SQL statement.
 * This function adds replacement string column to previously existed {@link SqlAdvisorGetHintsFunction}
 */
public class SqlAdvisorGetHintsFunction2 implements TableFunction, ImplementableFunction {

    private static final Expression ADVISOR =
            Expressions.convert_(
                    Expressions.call( DataContext.ROOT,
                            BuiltInMethod.DATA_CONTEXT_GET.method,
                            Expressions.constant( DataContext.Variable.SQL_ADVISOR.camelName ) ),
                    SqlAdvisor.class );

    private static final Method GET_COMPLETION_HINTS = Types.lookupMethod( SqlAdvisorGetHintsFunction2.class, "getCompletionHints", SqlAdvisor.class, String.class, int.class );

    private static final CallImplementor IMPLEMENTOR =
            RexImpTable.createImplementor(
                    ( translator, call, operands ) ->
                            Expressions.call( GET_COMPLETION_HINTS,
                                    Iterables.concat( Collections.singleton( ADVISOR ), operands ) ),
                    NullPolicy.ANY, false );

    private static final List<FunctionParameter> PARAMETERS =
            ReflectiveFunctionBase.builder()
                    .add( String.class, "sql" )
                    .add( int.class, "pos" )
                    .build();


    @Override
    public CallImplementor getImplementor() {
        return IMPLEMENTOR;
    }


    @Override
    public RelDataType getRowType( RelDataTypeFactory typeFactory, List<Object> arguments ) {
        return typeFactory.createJavaType( SqlAdvisorHint2.class );
    }


    @Override
    public Type getElementType( List<Object> arguments ) {
        return SqlAdvisorHint2.class;
    }


    @Override
    public List<FunctionParameter> getParameters() {
        return PARAMETERS;
    }


    /**
     * Returns completion hints for a given SQL statement.
     *
     * Typically this is called from generated code (via {@link SqlAdvisorGetHintsFunction2#IMPLEMENTOR}).
     *
     * @param advisor Advisor to produce completion hints
     * @param sql SQL to complete
     * @param pos Cursor position in SQL
     * @return the table that contains completion hints for a given SQL statement
     */
    public static Enumerable<SqlAdvisorHint2> getCompletionHints( final SqlAdvisor advisor, final String sql, final int pos ) {
        final String[] replaced = { null };
        final List<SqlMoniker> hints = advisor.getCompletionHints( sql, pos, replaced );
        final List<SqlAdvisorHint2> res = new ArrayList<>( hints.size() + 1 );
        res.add( new SqlAdvisorHint2( replaced[0], null, "MATCH", null ) );

        String word = replaced[0];
        for ( SqlMoniker hint : hints ) {
            res.add( new SqlAdvisorHint2( hint, advisor.getReplacement( hint, word ) ) );
        }
        return Linq4j.asEnumerable( res ).asQueryable();
    }
}

