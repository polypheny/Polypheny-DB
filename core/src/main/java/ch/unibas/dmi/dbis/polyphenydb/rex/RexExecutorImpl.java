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


import ch.unibas.dmi.dbis.polyphenydb.DataContext;
import ch.unibas.dmi.dbis.polyphenydb.adapter.enumerable.RexToLixTranslator;
import ch.unibas.dmi.dbis.polyphenydb.adapter.enumerable.RexToLixTranslator.InputGetter;
import ch.unibas.dmi.dbis.polyphenydb.adapter.java.JavaTypeFactory;
import ch.unibas.dmi.dbis.polyphenydb.config.RuntimeConfig;
import ch.unibas.dmi.dbis.polyphenydb.information.InformationCode;
import ch.unibas.dmi.dbis.polyphenydb.information.InformationGroup;
import ch.unibas.dmi.dbis.polyphenydb.information.InformationManager;
import ch.unibas.dmi.dbis.polyphenydb.information.InformationPage;
import ch.unibas.dmi.dbis.polyphenydb.jdbc.JavaTypeFactoryImpl;
import ch.unibas.dmi.dbis.polyphenydb.rel.type.RelDataType;
import ch.unibas.dmi.dbis.polyphenydb.rel.type.RelDataTypeFactory;
import ch.unibas.dmi.dbis.polyphenydb.sql.validate.SqlConformance;
import ch.unibas.dmi.dbis.polyphenydb.sql.validate.SqlConformanceEnum;
import ch.unibas.dmi.dbis.polyphenydb.util.BuiltInMethod;
import ch.unibas.dmi.dbis.polyphenydb.util.Util;
import com.google.common.collect.ImmutableList;
import java.lang.reflect.Modifier;
import java.lang.reflect.Type;
import java.util.List;
import org.apache.calcite.linq4j.tree.BlockBuilder;
import org.apache.calcite.linq4j.tree.Expression;
import org.apache.calcite.linq4j.tree.Expressions;
import org.apache.calcite.linq4j.tree.IndexExpression;
import org.apache.calcite.linq4j.tree.MethodCallExpression;
import org.apache.calcite.linq4j.tree.MethodDeclaration;
import org.apache.calcite.linq4j.tree.ParameterExpression;


/**
 * Evaluates a {@link RexNode} expression.
 */
public class RexExecutorImpl implements RexExecutor {

    private final DataContext dataContext;


    public RexExecutorImpl( DataContext dataContext ) {
        this.dataContext = dataContext;
    }


    private String compile( RexBuilder rexBuilder, List<RexNode> constExps, RexToLixTranslator.InputGetter getter ) {
        final RelDataTypeFactory typeFactory = rexBuilder.getTypeFactory();
        final RelDataType emptyRowType = typeFactory.builder().build();
        return compile( rexBuilder, constExps, getter, emptyRowType );
    }


    private String compile( RexBuilder rexBuilder, List<RexNode> constExps, RexToLixTranslator.InputGetter getter, RelDataType rowType ) {
        final RexProgramBuilder programBuilder = new RexProgramBuilder( rowType, rexBuilder );
        for ( RexNode node : constExps ) {
            programBuilder.addProject( node, "c" + programBuilder.getProjectList().size() );
        }
        final JavaTypeFactoryImpl javaTypeFactory = new JavaTypeFactoryImpl( rexBuilder.getTypeFactory().getTypeSystem() );
        final BlockBuilder blockBuilder = new BlockBuilder();
        final ParameterExpression root0_ = Expressions.parameter( Object.class, "root0" );
        final ParameterExpression root_ = DataContext.ROOT;
        blockBuilder.add(
                Expressions.declare(
                        Modifier.FINAL, root_,
                        Expressions.convert_( root0_, DataContext.class ) ) );
        final SqlConformance conformance = SqlConformanceEnum.DEFAULT;
        final RexProgram program = programBuilder.getProgram();
        final List<Expression> expressions =
                RexToLixTranslator.translateProjects(
                        program,
                        javaTypeFactory,
                        conformance,
                        blockBuilder,
                        null,
                        root_,
                        getter,
                        null );
        blockBuilder.add( Expressions.return_( null, Expressions.newArrayInit( Object[].class, expressions ) ) );
        final MethodDeclaration methodDecl =
                Expressions.methodDecl(
                        Modifier.PUBLIC,
                        Object[].class,
                        BuiltInMethod.FUNCTION1_APPLY.method.getName(),
                        ImmutableList.of( root0_ ),
                        blockBuilder.toBlock() );
        String code = Expressions.toString( methodDecl );
        if ( RuntimeConfig.DEBUG.getBoolean() ) {
            Util.debugCode( System.out, code );
        }
        if ( dataContext != null && dataContext.getTransaction() != null && dataContext.getTransaction().isAnalyze() ) {
            InformationManager queryAnalyzer = dataContext.getTransaction().getQueryAnalyzer();
            InformationPage page = new InformationPage( "informationPageGeneratedCode", "Generated Code" );
            InformationGroup group = new InformationGroup( page, "Generated Code" );
            queryAnalyzer.addPage( new InformationPage( "informationPageGeneratedCode", "Generated Code" ) );
            queryAnalyzer.addGroup( group );
            InformationCode informationCode = new InformationCode( group, code );
            queryAnalyzer.registerInformation( informationCode );
        }
        return code;
    }


    /**
     * Creates an {@link RexExecutable} that allows to apply the generated code during query processing (filter, projection).
     *
     * @param rexBuilder Rex builder
     * @param exps Expressions
     * @param rowType describes the structure of the input row.
     */
    public RexExecutable getExecutable( RexBuilder rexBuilder, List<RexNode> exps, RelDataType rowType ) {
        final JavaTypeFactoryImpl typeFactory = new JavaTypeFactoryImpl( rexBuilder.getTypeFactory().getTypeSystem() );
        final InputGetter getter = new DataContextInputGetter( rowType, typeFactory );
        final String code = compile( rexBuilder, exps, getter, rowType );
        return new RexExecutable( code, "generated Rex code" );
    }


    /**
     * Do constant reduction using generated code.
     */
    @Override
    public void reduce( RexBuilder rexBuilder, List<RexNode> constExps, List<RexNode> reducedValues ) {
        final String code = compile( rexBuilder, constExps,
                ( list, index, storageType ) -> {
                    throw new UnsupportedOperationException();
                } );

        final RexExecutable executable = new RexExecutable( code, constExps );
        executable.setDataContext( dataContext );
        executable.reduce( rexBuilder, constExps, reducedValues );
    }


    /**
     * Implementation of {@link ch.unibas.dmi.dbis.polyphenydb.adapter.enumerable.RexToLixTranslator.InputGetter} that reads the values of input fields by calling
     * <code>{@link DataContext#get}("inputRecord")</code>.
     */
    private static class DataContextInputGetter implements InputGetter {

        private final RelDataTypeFactory typeFactory;
        private final RelDataType rowType;


        DataContextInputGetter( RelDataType rowType, RelDataTypeFactory typeFactory ) {
            this.rowType = rowType;
            this.typeFactory = typeFactory;
        }


        @Override
        public Expression field( BlockBuilder list, int index, Type storageType ) {
            MethodCallExpression recFromCtx = Expressions.call(
                    DataContext.ROOT,
                    BuiltInMethod.DATA_CONTEXT_GET.method,
                    Expressions.constant( "inputRecord" ) );
            Expression recFromCtxCasted = RexToLixTranslator.convert( recFromCtx, Object[].class );
            IndexExpression recordAccess = Expressions.arrayIndex( recFromCtxCasted, Expressions.constant( index ) );
            if ( storageType == null ) {
                final RelDataType fieldType = rowType.getFieldList().get( index ).getType();
                storageType = ((JavaTypeFactory) typeFactory).getJavaClass( fieldType );
            }
            return RexToLixTranslator.convert( recordAccess, storageType );
        }
    }
}

