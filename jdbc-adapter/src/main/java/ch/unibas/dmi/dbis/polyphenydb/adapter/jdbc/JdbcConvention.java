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

package ch.unibas.dmi.dbis.polyphenydb.adapter.jdbc;


import ch.unibas.dmi.dbis.polyphenydb.plan.Convention;
import ch.unibas.dmi.dbis.polyphenydb.plan.RelOptPlanner;
import ch.unibas.dmi.dbis.polyphenydb.plan.RelOptRule;
import ch.unibas.dmi.dbis.polyphenydb.rel.rules.FilterSetOpTransposeRule;
import ch.unibas.dmi.dbis.polyphenydb.rel.rules.ProjectRemoveRule;
import ch.unibas.dmi.dbis.polyphenydb.sql.SqlDialect;
import org.apache.calcite.linq4j.tree.Expression;


/**
 * Calling convention for relational operations that occur in a JDBC database.
 *
 * The convention is a slight misnomer. The operations occur in whatever data-flow architecture the database uses internally. Nevertheless, the result pops out in JDBC.
 *
 * This is the only convention, thus far, that is not a singleton. Each instance contains a JDBC schema (and therefore a data source). If Polypheny-DB is working with two different databases, it would even make sense to convert
 * from "JDBC#A" convention to "JDBC#B", even though we don't do it currently. (That would involve asking database B to open a database link to database A.)
 *
 * As a result, converter rules from and two this convention need to be instantiated, at the start of planning, for each JDBC database in play.
 */
public class JdbcConvention extends Convention.Impl {

    /**
     * Cost of a JDBC node versus implementing an equivalent node in a "typical" calling convention.
     */
    public static final double COST_MULTIPLIER = 0.8d;

    public final SqlDialect dialect;
    public final Expression expression;
    public final JdbcPhysicalNameProvider physicalNameProvider;


    public JdbcConvention( SqlDialect dialect, Expression expression, String name, JdbcPhysicalNameProvider physicalNameProvider ) {
        super( "JDBC." + name, JdbcRel.class );
        this.dialect = dialect;
        this.expression = expression;
        this.physicalNameProvider = physicalNameProvider;
    }


    public static JdbcConvention of( SqlDialect dialect, Expression expression, String name, JdbcPhysicalNameProvider physicalNameProvider ) {
        return new JdbcConvention( dialect, expression, name, physicalNameProvider );
    }


    @Override
    public void register( RelOptPlanner planner ) {
        for ( RelOptRule rule : JdbcRules.rules( this ) ) {
            planner.addRule( rule );
        }
        planner.addRule( FilterSetOpTransposeRule.INSTANCE );
        planner.addRule( ProjectRemoveRule.INSTANCE );
    }
}
