/*
 * Copyright 2019-2024 The Polypheny Project
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

package org.polypheny.db.interpreter;


import com.google.common.collect.ImmutableList;
import java.io.IOException;
import java.io.StringReader;
import java.lang.reflect.Modifier;
import java.util.ArrayList;
import java.util.List;
import org.apache.calcite.linq4j.function.Function1;
import org.apache.calcite.linq4j.tree.BlockBuilder;
import org.apache.calcite.linq4j.tree.BlockStatement;
import org.apache.calcite.linq4j.tree.ClassDeclaration;
import org.apache.calcite.linq4j.tree.Expression;
import org.apache.calcite.linq4j.tree.Expressions;
import org.apache.calcite.linq4j.tree.MemberDeclaration;
import org.apache.calcite.linq4j.tree.ParameterExpression;
import org.codehaus.commons.compiler.CompileException;
import org.codehaus.commons.compiler.CompilerFactoryFactory;
import org.codehaus.commons.compiler.IClassBodyEvaluator;
import org.codehaus.commons.compiler.ICompilerFactory;
import org.polypheny.db.adapter.DataContext;
import org.polypheny.db.algebra.constant.ConformanceEnum;
import org.polypheny.db.algebra.enumerable.JavaTupleFormat;
import org.polypheny.db.algebra.enumerable.PhysTypeImpl;
import org.polypheny.db.algebra.enumerable.RexToLixTranslator;
import org.polypheny.db.algebra.enumerable.RexToLixTranslator.InputGetter;
import org.polypheny.db.algebra.enumerable.RexToLixTranslator.InputGetterImpl;
import org.polypheny.db.algebra.type.AlgDataType;
import org.polypheny.db.config.RuntimeConfig;
import org.polypheny.db.information.InformationCode;
import org.polypheny.db.information.InformationGroup;
import org.polypheny.db.information.InformationManager;
import org.polypheny.db.information.InformationPage;
import org.polypheny.db.prepare.JavaTypeFactoryImpl;
import org.polypheny.db.rex.RexBuilder;
import org.polypheny.db.rex.RexNode;
import org.polypheny.db.rex.RexProgram;
import org.polypheny.db.rex.RexProgramBuilder;
import org.polypheny.db.type.entity.PolyValue;
import org.polypheny.db.util.BuiltInMethod;
import org.polypheny.db.util.Conformance;
import org.polypheny.db.util.Pair;
import org.polypheny.db.util.Util;


/**
 * Compiles a scalar expression ({@link RexNode}) to an expression that can be evaluated ({@link Scalar}) by generating a Java AST and compiling it to a class using Janino.
 */
public class JaninoRexCompiler implements Interpreter.ScalarCompiler {

    private final RexBuilder rexBuilder;


    public JaninoRexCompiler( RexBuilder rexBuilder ) {
        this.rexBuilder = rexBuilder;
    }


    @Override
    public Scalar compile( List<RexNode> nodes, AlgDataType inputRowType, DataContext dataContext ) {
        final RexProgramBuilder programBuilder = new RexProgramBuilder( inputRowType, rexBuilder );
        for ( RexNode node : nodes ) {
            programBuilder.addProject( node, null );
        }
        final RexProgram program = programBuilder.getProgram();

        final BlockBuilder builder = new BlockBuilder();
        final ParameterExpression context_ = Expressions.parameter( Context.class, "context" );
        final ParameterExpression outputValues_ = Expressions.parameter( PolyValue[].class, "outputValues" );
        final JavaTypeFactoryImpl javaTypeFactory = new JavaTypeFactoryImpl( rexBuilder.getTypeFactory().getTypeSystem() );

        // public void execute(Context, Object[] outputValues)
        final InputGetter inputGetter =
                new InputGetterImpl(
                        ImmutableList.of(
                                Pair.of(
                                        Expressions.field( context_, BuiltInMethod.CONTEXT_VALUES.field ),
                                        PhysTypeImpl.of( javaTypeFactory, inputRowType, JavaTupleFormat.ARRAY, false ) ) ) );
        final Function1<String, InputGetter> correlates = a0 -> {
            throw new UnsupportedOperationException();
        };
        final Expression root = Expressions.field( context_, BuiltInMethod.CONTEXT_ROOT.field );
        final Conformance conformance = ConformanceEnum.DEFAULT; // TODO: get this from implementor
        final List<Expression> list = RexToLixTranslator.translateProjects( program, javaTypeFactory, conformance, builder, null, root, inputGetter, correlates );
        for ( int i = 0; i < list.size(); i++ ) {
            builder.add(
                    Expressions.statement(
                            Expressions.assign(
                                    Expressions.arrayIndex( outputValues_, Expressions.constant( i ) ), list.get( i ) ) ) );
        }
        return baz( context_, outputValues_, builder.toBlock(), dataContext );
    }


    /**
     * Given a method that implements {@link Scalar#execute(Context, org.polypheny.db.type.entity.PolyValue[])}, adds a bridge method that implements {@link Scalar#execute(Context)}, and compiles.
     */
    static Scalar baz( ParameterExpression context_, ParameterExpression outputValues_, BlockStatement block, DataContext dataContext ) {
        final List<MemberDeclaration> declarations = new ArrayList<>();

        // public void execute(Context, Object[] outputValues)
        declarations.add( Expressions.methodDecl( Modifier.PUBLIC, void.class, BuiltInMethod.SCALAR_EXECUTE2.method.getName(), ImmutableList.of( context_, outputValues_ ), block ) );

        // public Object execute(Context)
        final BlockBuilder builder = new BlockBuilder();
        final Expression values_ = builder.append( "values", Expressions.newArrayBounds( PolyValue.class, 1, Expressions.constant( 1 ) ) );
        builder.add(
                Expressions.statement(
                        Expressions.call(
                                Expressions.parameter( Scalar.class, "this" ),
                                BuiltInMethod.SCALAR_EXECUTE2.method, context_, values_ ) ) );
        builder.add( Expressions.return_( null, Expressions.arrayIndex( values_, Expressions.constant( 0 ) ) ) );
        declarations.add( Expressions.methodDecl( Modifier.PUBLIC, PolyValue.class, BuiltInMethod.SCALAR_EXECUTE1.method.getName(), ImmutableList.of( context_ ), builder.toBlock() ) );

        final ClassDeclaration classDeclaration = Expressions.classDecl( Modifier.PUBLIC, "Buzz", null, ImmutableList.of( Scalar.class ), declarations );
        String s = Expressions.toString( declarations, "\n", false );
        if ( RuntimeConfig.DEBUG.getBoolean() ) {
            Util.debugCode( System.out, s );
        }
        if ( dataContext != null && dataContext.getStatement() != null && dataContext.getStatement().getTransaction().isAnalyze() ) {
            InformationManager queryAnalyzer = dataContext.getStatement().getTransaction().getQueryAnalyzer();
            InformationPage page = new InformationPage( "Generated Code" );
            page.fullWidth();
            InformationGroup group = new InformationGroup( page, "Generated Code" );
            queryAnalyzer.addPage( page );
            queryAnalyzer.addGroup( group );
            InformationCode informationCode = new InformationCode( group, s );
            queryAnalyzer.registerInformation( informationCode );
        }
        try {
            return getScalar( classDeclaration, s );
        } catch ( CompileException | IOException e ) {
            throw new RuntimeException( e );
        }
    }


    static Scalar getScalar( ClassDeclaration expr, String s ) throws CompileException, IOException {
        ICompilerFactory compilerFactory;
        try {
            compilerFactory = CompilerFactoryFactory.getDefaultCompilerFactory();
        } catch ( Exception e ) {
            throw new IllegalStateException( "Unable to instantiate java compiler", e );
        }
        IClassBodyEvaluator cbe = compilerFactory.newClassBodyEvaluator();
        cbe.setClassName( expr.name );
        cbe.setImplementedInterfaces( new Class[]{ Scalar.class } );
        cbe.setParentClassLoader( JaninoRexCompiler.class.getClassLoader() );
        if ( RuntimeConfig.DEBUG.getBoolean() ) {
            // Add line numbers to the generated janino class
            cbe.setDebuggingInformation( true, true, true );
        }
        return (Scalar) cbe.createInstance( new StringReader( s ) );
    }

}

