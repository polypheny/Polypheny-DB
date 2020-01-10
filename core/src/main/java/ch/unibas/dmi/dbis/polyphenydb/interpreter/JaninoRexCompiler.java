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

package ch.unibas.dmi.dbis.polyphenydb.interpreter;


import ch.unibas.dmi.dbis.polyphenydb.DataContext;
import ch.unibas.dmi.dbis.polyphenydb.adapter.enumerable.JavaRowFormat;
import ch.unibas.dmi.dbis.polyphenydb.adapter.enumerable.PhysTypeImpl;
import ch.unibas.dmi.dbis.polyphenydb.adapter.enumerable.RexToLixTranslator;
import ch.unibas.dmi.dbis.polyphenydb.adapter.enumerable.RexToLixTranslator.InputGetter;
import ch.unibas.dmi.dbis.polyphenydb.adapter.enumerable.RexToLixTranslator.InputGetterImpl;
import ch.unibas.dmi.dbis.polyphenydb.config.RuntimeConfig;
import ch.unibas.dmi.dbis.polyphenydb.information.InformationCode;
import ch.unibas.dmi.dbis.polyphenydb.information.InformationGroup;
import ch.unibas.dmi.dbis.polyphenydb.information.InformationManager;
import ch.unibas.dmi.dbis.polyphenydb.information.InformationPage;
import ch.unibas.dmi.dbis.polyphenydb.jdbc.JavaTypeFactoryImpl;
import ch.unibas.dmi.dbis.polyphenydb.rel.type.RelDataType;
import ch.unibas.dmi.dbis.polyphenydb.rex.RexBuilder;
import ch.unibas.dmi.dbis.polyphenydb.rex.RexNode;
import ch.unibas.dmi.dbis.polyphenydb.rex.RexProgram;
import ch.unibas.dmi.dbis.polyphenydb.rex.RexProgramBuilder;
import ch.unibas.dmi.dbis.polyphenydb.sql.validate.SqlConformance;
import ch.unibas.dmi.dbis.polyphenydb.sql.validate.SqlConformanceEnum;
import ch.unibas.dmi.dbis.polyphenydb.util.BuiltInMethod;
import ch.unibas.dmi.dbis.polyphenydb.util.Pair;
import ch.unibas.dmi.dbis.polyphenydb.util.Util;
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


/**
 * Compiles a scalar expression ({@link RexNode}) to an expression that can be evaluated ({@link Scalar}) by generating a Java AST and compiling it to a class using Janino.
 */
public class JaninoRexCompiler implements Interpreter.ScalarCompiler {

    private final RexBuilder rexBuilder;


    public JaninoRexCompiler( RexBuilder rexBuilder ) {
        this.rexBuilder = rexBuilder;
    }


    @Override
    public Scalar compile( List<RexNode> nodes, RelDataType inputRowType, DataContext dataContext ) {
        final RexProgramBuilder programBuilder = new RexProgramBuilder( inputRowType, rexBuilder );
        for ( RexNode node : nodes ) {
            programBuilder.addProject( node, null );
        }
        final RexProgram program = programBuilder.getProgram();

        final BlockBuilder builder = new BlockBuilder();
        final ParameterExpression context_ = Expressions.parameter( Context.class, "context" );
        final ParameterExpression outputValues_ = Expressions.parameter( Object[].class, "outputValues" );
        final JavaTypeFactoryImpl javaTypeFactory = new JavaTypeFactoryImpl( rexBuilder.getTypeFactory().getTypeSystem() );

        // public void execute(Context, Object[] outputValues)
        final InputGetter inputGetter =
                new InputGetterImpl(
                        ImmutableList.of(
                                Pair.of(
                                        Expressions.field( context_, BuiltInMethod.CONTEXT_VALUES.field ),
                                        PhysTypeImpl.of( javaTypeFactory, inputRowType, JavaRowFormat.ARRAY, false ) ) ) );
        final Function1<String, InputGetter> correlates = a0 -> {
            throw new UnsupportedOperationException();
        };
        final Expression root = Expressions.field( context_, BuiltInMethod.CONTEXT_ROOT.field );
        final SqlConformance conformance = SqlConformanceEnum.DEFAULT; // TODO: get this from implementor
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
     * Given a method that implements {@link Scalar#execute(Context, Object[])}, adds a bridge method that implements {@link Scalar#execute(Context)}, and compiles.
     */
    static Scalar baz( ParameterExpression context_, ParameterExpression outputValues_, BlockStatement block, DataContext dataContext ) {
        final List<MemberDeclaration> declarations = new ArrayList<>();

        // public void execute(Context, Object[] outputValues)
        declarations.add( Expressions.methodDecl( Modifier.PUBLIC, void.class, BuiltInMethod.SCALAR_EXECUTE2.method.getName(), ImmutableList.of( context_, outputValues_ ), block ) );

        // public Object execute(Context)
        final BlockBuilder builder = new BlockBuilder();
        final Expression values_ = builder.append( "values", Expressions.newArrayBounds( Object.class, 1, Expressions.constant( 1 ) ) );
        builder.add(
                Expressions.statement(
                        Expressions.call(
                                Expressions.parameter( Scalar.class, "this" ),
                                BuiltInMethod.SCALAR_EXECUTE2.method, context_, values_ ) ) );
        builder.add( Expressions.return_( null, Expressions.arrayIndex( values_, Expressions.constant( 0 ) ) ) );
        declarations.add( Expressions.methodDecl( Modifier.PUBLIC, Object.class, BuiltInMethod.SCALAR_EXECUTE1.method.getName(), ImmutableList.of( context_ ), builder.toBlock() ) );

        final ClassDeclaration classDeclaration = Expressions.classDecl( Modifier.PUBLIC, "Buzz", null, ImmutableList.of( Scalar.class ), declarations );
        String s = Expressions.toString( declarations, "\n", false );
        if ( RuntimeConfig.DEBUG.getBoolean() ) {
            Util.debugCode( System.out, s );
        }
        if ( dataContext != null && dataContext.getTransaction() != null && dataContext.getTransaction().isAnalyze() ) {
            InformationManager queryAnalyzer = dataContext.getTransaction().getQueryAnalyzer();
            InformationPage page = new InformationPage( "informationPageGeneratedCode", "Generated Code" );
            InformationGroup group = new InformationGroup( page, "Generated Code" );
            queryAnalyzer.addPage( new InformationPage( "informationPageGeneratedCode", "Generated Code" ) );
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

