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

package ch.unibas.dmi.dbis.polyphenydb.adapter.enumerable;


import ch.unibas.dmi.dbis.polyphenydb.DataContext;
import ch.unibas.dmi.dbis.polyphenydb.interpreter.BindableConvention;
import ch.unibas.dmi.dbis.polyphenydb.interpreter.BindableRel;
import ch.unibas.dmi.dbis.polyphenydb.interpreter.Node;
import ch.unibas.dmi.dbis.polyphenydb.interpreter.Row;
import ch.unibas.dmi.dbis.polyphenydb.interpreter.Sink;
import ch.unibas.dmi.dbis.polyphenydb.plan.ConventionTraitDef;
import ch.unibas.dmi.dbis.polyphenydb.plan.RelOptCluster;
import ch.unibas.dmi.dbis.polyphenydb.plan.RelTraitSet;
import ch.unibas.dmi.dbis.polyphenydb.rel.RelNode;
import ch.unibas.dmi.dbis.polyphenydb.rel.convert.ConverterImpl;
import ch.unibas.dmi.dbis.polyphenydb.rel.convert.ConverterRule;
import ch.unibas.dmi.dbis.polyphenydb.rel.core.RelFactories;
import ch.unibas.dmi.dbis.polyphenydb.runtime.ArrayBindable;
import ch.unibas.dmi.dbis.polyphenydb.runtime.Bindable;
import ch.unibas.dmi.dbis.polyphenydb.tools.RelBuilderFactory;
import com.google.common.collect.ImmutableMap;
import java.util.List;
import java.util.function.Predicate;
import org.apache.calcite.linq4j.Enumerable;
import org.apache.calcite.linq4j.Enumerator;


/**
 * Relational expression that converts an enumerable input to interpretable calling convention.
 *
 * @see ch.unibas.dmi.dbis.polyphenydb.adapter.enumerable.EnumerableConvention
 * @see ch.unibas.dmi.dbis.polyphenydb.interpreter.BindableConvention
 */
public class EnumerableBindable extends ConverterImpl implements BindableRel {

    protected EnumerableBindable( RelOptCluster cluster, RelNode input ) {
        super( cluster, ConventionTraitDef.INSTANCE, cluster.traitSetOf( BindableConvention.INSTANCE ), input );
    }


    @Override
    public EnumerableBindable copy( RelTraitSet traitSet, List<RelNode> inputs ) {
        return new EnumerableBindable( getCluster(), sole( inputs ) );
    }


    @Override
    public Class<Object[]> getElementType() {
        return Object[].class;
    }


    @Override
    public Enumerable<Object[]> bind( DataContext dataContext ) {
        final ImmutableMap<String, Object> map = ImmutableMap.of();
        final Bindable bindable = EnumerableInterpretable.toBindable( map, null, (EnumerableRel) getInput(), EnumerableRel.Prefer.ARRAY );
        final ArrayBindable arrayBindable = EnumerableInterpretable.box( bindable );
        return arrayBindable.bind( dataContext );
    }


    @Override
    public Node implement( final InterpreterImplementor implementor ) {
        return () -> {
            final Sink sink = implementor.relSinks.get( EnumerableBindable.this ).get( 0 );
            final Enumerable<Object[]> enumerable = bind( implementor.dataContext );
            final Enumerator<Object[]> enumerator = enumerable.enumerator();
            while ( enumerator.moveNext() ) {
                sink.send( Row.asCopy( enumerator.current() ) );
            }
        };
    }


    /**
     * Rule that converts any enumerable relational expression to bindable.
     */
    public static class EnumerableToBindableConverterRule extends ConverterRule {

        public static final EnumerableToBindableConverterRule INSTANCE = new EnumerableToBindableConverterRule( RelFactories.LOGICAL_BUILDER );


        /**
         * Creates an EnumerableToBindableConverterRule.
         *
         * @param relBuilderFactory Builder for relational expressions
         */
        public EnumerableToBindableConverterRule( RelBuilderFactory relBuilderFactory ) {
            super( EnumerableRel.class, (Predicate<RelNode>) r -> true, EnumerableConvention.INSTANCE, BindableConvention.INSTANCE, relBuilderFactory, "EnumerableToBindableConverterRule" );
        }


        @Override
        public RelNode convert( RelNode rel ) {
            return new EnumerableBindable( rel.getCluster(), rel );
        }
    }
}

