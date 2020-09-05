/*
 * Copyright 2019-2020 The Polypheny Project
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
 *
 * This file incorporates code covered by the following terms:
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
 */

package org.polypheny.db.adapter.enumerable;


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
import org.polypheny.db.adapter.DataContext;
import org.polypheny.db.config.RuntimeConfig;
import org.polypheny.db.information.InformationCode;
import org.polypheny.db.information.InformationGroup;
import org.polypheny.db.information.InformationManager;
import org.polypheny.db.information.InformationPage;
import org.polypheny.db.interpreter.BindableConvention;
import org.polypheny.db.interpreter.Compiler;
import org.polypheny.db.interpreter.InterpretableConvention;
import org.polypheny.db.interpreter.InterpretableRel;
import org.polypheny.db.interpreter.Node;
import org.polypheny.db.interpreter.Row;
import org.polypheny.db.interpreter.Sink;
import org.polypheny.db.jdbc.PolyphenyDbPrepare.SparkHandler;
import org.polypheny.db.plan.ConventionTraitDef;
import org.polypheny.db.plan.RelOptCluster;
import org.polypheny.db.plan.RelTraitSet;
import org.polypheny.db.rel.RelNode;
import org.polypheny.db.rel.convert.ConverterImpl;
import org.polypheny.db.runtime.ArrayBindable;
import org.polypheny.db.runtime.Bindable;
import org.polypheny.db.runtime.Hook;
import org.polypheny.db.runtime.Typed;
import org.polypheny.db.runtime.Utilities;
import org.polypheny.db.transaction.Statement;
import org.polypheny.db.util.Util;


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
        final Bindable bindable = toBindable(
                implementor.internalParameters,
                implementor.spark,
                (EnumerableRel) getInput(),
                EnumerableRel.Prefer.ARRAY,
                implementor.dataContext.getStatement() );
        final ArrayBindable arrayBindable = box( bindable );
        final Enumerable<Object[]> enumerable = arrayBindable.bind( implementor.dataContext );
        return new EnumerableNode( enumerable, implementor.compiler, this );
    }


    public static Bindable toBindable( Map<String, Object> parameters, SparkHandler spark, EnumerableRel rel, EnumerableRel.Prefer prefer, Statement statement ) {
        EnumerableRelImplementor relImplementor = new EnumerableRelImplementor( rel.getCluster().getRexBuilder(), parameters );

        final ClassDeclaration expr = relImplementor.implementRoot( rel, prefer );
        String s = Expressions.toString( expr.memberDeclarations, "\n", false );

        if ( RuntimeConfig.DEBUG.getBoolean() ) {
            Util.debugCode( System.out, s );
        }

        if ( statement != null && statement.getTransaction().isAnalyze() ) {
            InformationManager queryAnalyzer = statement.getTransaction().getQueryAnalyzer();
            InformationPage page = new InformationPage( "Generated Code" );
            page.fullWidth();
            InformationGroup group = new InformationGroup( page, "Generated Code" );
            queryAnalyzer.addPage( page );
            queryAnalyzer.addGroup( group );
            InformationCode informationCode = new InformationCode( group, s );
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
        if ( RuntimeConfig.DEBUG.getBoolean() ) {
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

