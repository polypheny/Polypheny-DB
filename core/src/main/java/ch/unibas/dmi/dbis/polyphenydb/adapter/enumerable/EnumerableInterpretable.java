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
import ch.unibas.dmi.dbis.polyphenydb.information.InformationCode;
import ch.unibas.dmi.dbis.polyphenydb.information.InformationGroup;
import ch.unibas.dmi.dbis.polyphenydb.information.InformationManager;
import ch.unibas.dmi.dbis.polyphenydb.information.InformationPage;
import ch.unibas.dmi.dbis.polyphenydb.interpreter.BindableConvention;
import ch.unibas.dmi.dbis.polyphenydb.interpreter.Compiler;
import ch.unibas.dmi.dbis.polyphenydb.interpreter.InterpretableConvention;
import ch.unibas.dmi.dbis.polyphenydb.interpreter.InterpretableRel;
import ch.unibas.dmi.dbis.polyphenydb.interpreter.Node;
import ch.unibas.dmi.dbis.polyphenydb.interpreter.Row;
import ch.unibas.dmi.dbis.polyphenydb.interpreter.Sink;
import ch.unibas.dmi.dbis.polyphenydb.jdbc.PolyphenyDbPrepare.SparkHandler;
import ch.unibas.dmi.dbis.polyphenydb.plan.ConventionTraitDef;
import ch.unibas.dmi.dbis.polyphenydb.plan.RelOptCluster;
import ch.unibas.dmi.dbis.polyphenydb.plan.RelTraitSet;
import ch.unibas.dmi.dbis.polyphenydb.prepare.PolyphenyDbPrepareImpl;
import ch.unibas.dmi.dbis.polyphenydb.rel.RelNode;
import ch.unibas.dmi.dbis.polyphenydb.rel.convert.ConverterImpl;
import ch.unibas.dmi.dbis.polyphenydb.runtime.ArrayBindable;
import ch.unibas.dmi.dbis.polyphenydb.runtime.Bindable;
import ch.unibas.dmi.dbis.polyphenydb.runtime.Hook;
import ch.unibas.dmi.dbis.polyphenydb.runtime.Typed;
import ch.unibas.dmi.dbis.polyphenydb.runtime.Utilities;
import ch.unibas.dmi.dbis.polyphenydb.util.Util;
import java.io.IOException;
import java.io.StringReader;
import java.util.List;
import java.util.Map;
import org.apache.calcite.avatica.Helper;
import org.apache.calcite.linq4j.AbstractEnumerable;
import org.apache.calcite.linq4j.Enumerable;
import org.apache.calcite.linq4j.Enumerator;
import org.apache.calcite.linq4j.tree.ClassDeclaration;
import org.apache.calcite.linq4j.tree.Expressions;
import org.codehaus.commons.compiler.CompileException;
import org.codehaus.commons.compiler.CompilerFactoryFactory;
import org.codehaus.commons.compiler.IClassBodyEvaluator;
import org.codehaus.commons.compiler.ICompilerFactory;


/**
 * Relational expression that converts an enumerable input to interpretable calling convention.
 *
 * @see EnumerableConvention
 * @see BindableConvention
 */
public class EnumerableInterpretable extends ConverterImpl implements InterpretableRel {

    protected EnumerableInterpretable( RelOptCluster cluster, RelNode input ) {
        super( cluster, ConventionTraitDef.INSTANCE, cluster.traitSetOf( InterpretableConvention.INSTANCE ), input );
    }


    @Override
    public EnumerableInterpretable copy( RelTraitSet traitSet, List<RelNode> inputs ) {
        return new EnumerableInterpretable( getCluster(), sole( inputs ) );
    }


    @Override
    public Node implement( final InterpreterImplementor implementor ) {
        final Bindable bindable = toBindable( implementor.internalParameters, implementor.spark, (EnumerableRel) getInput(), EnumerableRel.Prefer.ARRAY, implementor.dataContext );
        final ArrayBindable arrayBindable = box( bindable );
        final Enumerable<Object[]> enumerable = arrayBindable.bind( implementor.dataContext );
        return new EnumerableNode( enumerable, implementor.compiler, this );
    }


    public static Bindable toBindable( Map<String, Object> parameters, SparkHandler spark, EnumerableRel rel, EnumerableRel.Prefer prefer, DataContext dataContext ) {
        EnumerableRelImplementor relImplementor = new EnumerableRelImplementor( rel.getCluster().getRexBuilder(), parameters );

        final ClassDeclaration expr = relImplementor.implementRoot( rel, prefer );
        String s = Expressions.toString( expr.memberDeclarations, "\n", false );

        if ( PolyphenyDbPrepareImpl.DEBUG ) {
            Util.debugCode( System.out, s );
        }

        if ( dataContext != null && dataContext.getTransaction().isAnalyze() ) {
            InformationManager queryAnalyzer = dataContext.getTransaction().getQueryAnalyzer();
            InformationPage page = new InformationPage( "informationPageGeneratedCode", "Generated Code" );
            InformationGroup group = new InformationGroup( page, "Generated Code" );
            queryAnalyzer.addPage( new InformationPage( "informationPageGeneratedCode", "Generated Code" ) );
            queryAnalyzer.addGroup( group );
            InformationCode informationCode = new InformationCode(
                    group,
                    s );
            queryAnalyzer.registerInformation( informationCode );
        }

        Hook.JAVA_PLAN.run( s );

        try {
            if ( spark != null && spark.enabled() ) {
                return spark.compile( expr, s );
            } else {
                return getBindable( expr, s, rel.getRowType().getFieldCount() );
            }
        } catch ( Exception e ) {
            throw Helper.INSTANCE.wrap( "Error while compiling generated Java code:\n" + s, e );
        }
    }


    static ArrayBindable getArrayBindable( ClassDeclaration expr, String s, int fieldCount ) throws CompileException, IOException {
        Bindable bindable = getBindable( expr, s, fieldCount );
        return box( bindable );
    }


    static Bindable getBindable( ClassDeclaration expr, String s, int fieldCount ) throws CompileException, IOException {
        ICompilerFactory compilerFactory;
        try {
            compilerFactory = CompilerFactoryFactory.getDefaultCompilerFactory();
        } catch ( Exception e ) {
            throw new IllegalStateException( "Unable to instantiate java compiler", e );
        }
        IClassBodyEvaluator cbe = compilerFactory.newClassBodyEvaluator();
        cbe.setClassName( expr.name );
        cbe.setExtendedClass( Utilities.class );
        cbe.setImplementedInterfaces(
                fieldCount == 1
                        ? new Class[]{ Bindable.class, Typed.class }
                        : new Class[]{ ArrayBindable.class } );
        cbe.setParentClassLoader( EnumerableInterpretable.class.getClassLoader() );
        if ( PolyphenyDbPrepareImpl.DEBUG ) {
            // Add line numbers to the generated janino class
            cbe.setDebuggingInformation( true, true, true );
        }
        return (Bindable) cbe.createInstance( new StringReader( s ) );
    }


    /**
     * Converts a bindable over scalar values into an array bindable, with each row as an array of 1 element.
     */
    static ArrayBindable box( final Bindable bindable ) {
        if ( bindable instanceof ArrayBindable ) {
            return (ArrayBindable) bindable;
        }
        return new ArrayBindable() {
            @Override
            public Class<Object[]> getElementType() {
                return Object[].class;
            }


            @Override
            public Enumerable<Object[]> bind( DataContext dataContext ) {
                final Enumerable<?> enumerable = bindable.bind( dataContext );
                return new AbstractEnumerable<Object[]>() {
                    @Override
                    public Enumerator<Object[]> enumerator() {
                        final Enumerator<?> enumerator = enumerable.enumerator();
                        return new Enumerator<Object[]>() {
                            @Override
                            public Object[] current() {
                                return new Object[]{ enumerator.current() };
                            }


                            @Override
                            public boolean moveNext() {
                                return enumerator.moveNext();
                            }


                            @Override
                            public void reset() {
                                enumerator.reset();
                            }


                            @Override
                            public void close() {
                                enumerator.close();
                            }
                        };
                    }
                };
            }
        };
    }


    /**
     * Interpreter node that reads from an {@link Enumerable}.
     *
     * From the interpreter's perspective, it is a leaf node.
     */
    private static class EnumerableNode implements Node {

        private final Enumerable<Object[]> enumerable;
        private final Sink sink;


        EnumerableNode( Enumerable<Object[]> enumerable, Compiler compiler, EnumerableInterpretable rel ) {
            this.enumerable = enumerable;
            this.sink = compiler.sink( rel );
        }


        @Override
        public void run() throws InterruptedException {
            final Enumerator<Object[]> enumerator = enumerable.enumerator();
            while ( enumerator.moveNext() ) {
                Object[] values = enumerator.current();
                sink.send( Row.of( values ) );
            }
        }
    }
}

