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

package ch.unibas.dmi.dbis.polyphenydb.adapter.pig;


import ch.unibas.dmi.dbis.polyphenydb.adapter.enumerable.EnumerableRel;
import ch.unibas.dmi.dbis.polyphenydb.adapter.enumerable.EnumerableRelImplementor;
import ch.unibas.dmi.dbis.polyphenydb.adapter.enumerable.JavaRowFormat;
import ch.unibas.dmi.dbis.polyphenydb.adapter.enumerable.PhysType;
import ch.unibas.dmi.dbis.polyphenydb.adapter.enumerable.PhysTypeImpl;
import ch.unibas.dmi.dbis.polyphenydb.plan.ConventionTraitDef;
import ch.unibas.dmi.dbis.polyphenydb.plan.RelOptCluster;
import ch.unibas.dmi.dbis.polyphenydb.plan.RelTraitSet;
import ch.unibas.dmi.dbis.polyphenydb.rel.AbstractRelNode;
import ch.unibas.dmi.dbis.polyphenydb.rel.RelNode;
import ch.unibas.dmi.dbis.polyphenydb.rel.convert.ConverterImpl;
import ch.unibas.dmi.dbis.polyphenydb.runtime.Hook;
import ch.unibas.dmi.dbis.polyphenydb.util.BuiltInMethod;
import java.util.List;
import org.apache.calcite.linq4j.tree.BlockBuilder;
import org.apache.calcite.linq4j.tree.Expressions;


/**
 * Relational expression representing a scan of a table in a Pig data source.
 */
public class PigToEnumerableConverter extends ConverterImpl implements EnumerableRel {

    /**
     * Creates a PigToEnumerableConverter.
     */
    protected PigToEnumerableConverter( RelOptCluster cluster, RelTraitSet traits, RelNode input ) {
        super( cluster, ConventionTraitDef.INSTANCE, traits, input );
    }


    @Override
    public RelNode copy( RelTraitSet traitSet, List<RelNode> inputs ) {
        return new PigToEnumerableConverter( getCluster(), traitSet, AbstractRelNode.sole( inputs ) );
    }


    /**
     * {@inheritDoc}
     *
     * This implementation does not actually execute the associated Pig Latin script and return results. Instead it returns an empty {@link Result}
     * in order to allow for testing and verification of every step of query processing up to actual physical execution and result verification.
     *
     * Next step is to invoke Pig from here, likely in local mode, have it store results in a predefined file so they can be read here and returned as a {@link ch.unibas.dmi.dbis.polyphenydb.adapter.enumerable.EnumerableRel.Result} object.
     */
    @Override
    public Result implement( EnumerableRelImplementor implementor, Prefer pref ) {
        final BlockBuilder list = new BlockBuilder();
        final PhysType physType = PhysTypeImpl.of( implementor.getTypeFactory(), rowType, pref.prefer( JavaRowFormat.ARRAY ) );
        PigRel.Implementor impl = new PigRel.Implementor();
        impl.visitChild( 0, getInput() );
        Hook.QUERY_PLAN.run( impl.getScript() ); // for script validation in tests
        list.add( Expressions.return_( null, Expressions.call( BuiltInMethod.EMPTY_ENUMERABLE.method ) ) );
        return implementor.result( physType, list.toBlock() );
    }
}
