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

package org.polypheny.db.rex;


import com.google.common.collect.ImmutableList;
import java.lang.reflect.Modifier;
import java.lang.reflect.Type;
import java.util.List;
import org.apache.calcite.linq4j.tree.BlockBuilder;
import org.apache.calcite.linq4j.tree.Expression;
import org.apache.calcite.linq4j.tree.Expressions;
import org.apache.calcite.linq4j.tree.IndexExpression;
import org.apache.calcite.linq4j.tree.MethodDeclaration;
import org.apache.calcite.linq4j.tree.ParameterExpression;
import org.polypheny.db.adapter.DataContext;
import org.polypheny.db.adapter.java.JavaTypeFactory;
import org.polypheny.db.algebra.constant.ConformanceEnum;
import org.polypheny.db.algebra.enumerable.RexToLixTranslator;
import org.polypheny.db.algebra.enumerable.RexToLixTranslator.InputGetter;
import org.polypheny.db.algebra.type.AlgDataType;
import org.polypheny.db.algebra.type.AlgDataTypeFactory;
import org.polypheny.db.config.RuntimeConfig;
import org.polypheny.db.information.InformationCode;
import org.polypheny.db.information.InformationGroup;
import org.polypheny.db.information.InformationManager;
import org.polypheny.db.information.InformationPage;
import org.polypheny.db.prepare.JavaTypeFactoryImpl;
import org.polypheny.db.type.entity.PolyValue;
import org.polypheny.db.util.BuiltInMethod;
import org.polypheny.db.util.Conformance;
import org.polypheny.db.util.Util;


/**
 * Evaluates a {@link RexNode} expression.
 */
public class RexExecutorImpl implements RexExecutor {

    private final DataContext dataContext;


    public RexExecutorImpl( DataContext dataContext ) {
        this.dataContext = dataContext;
    }


    private String compile( RexBuilder rexBuilder, List<RexNode> constExps, RexToLixTranslator.InputGetter getter ) {
        final AlgDataTypeFactory typeFactory = rexBuilder.getTypeFactory();
        final AlgDataType emptyRowType = typeFactory.builder().build();
        return compile( rexBuilder, constExps, getter, emptyRowType );
    }


    private String compile( RexBuilder rexBuilder, List<RexNode> constExps, RexToLixTranslator.InputGetter getter, AlgDataType rowType ) {
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
        final Conformance conformance = ConformanceEnum.DEFAULT;
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
        blockBuilder.add( Expressions.return_( null, Expressions.newArrayInit( PolyValue[].class, expressions ) ) );
        final MethodDeclaration methodDecl =
                Expressions.methodDecl(
                        Modifier.PUBLIC,
                        PolyValue[].class,
                        BuiltInMethod.FUNCTION1_APPLY.method.getName(),
                        ImmutableList.of( root0_ ),
                        blockBuilder.toBlock() );
        String code = Expressions.toString( methodDecl );
        if ( RuntimeConfig.DEBUG.getBoolean() ) {
            Util.debugCode( System.out, code );
        }
        if ( dataContext != null && dataContext.getStatement() != null && dataContext.getStatement().getTransaction().isAnalyze() ) {
            InformationManager queryAnalyzer = dataContext.getStatement().getTransaction().getQueryAnalyzer();
            InformationPage page = new InformationPage( "Generated Code" );
            page.fullWidth();
            InformationGroup group = new InformationGroup( page, "Generated Code" );
            queryAnalyzer.addPage( page );
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
    public RexExecutable getExecutable( RexBuilder rexBuilder, List<RexNode> exps, AlgDataType rowType ) {
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
     * Implementation of {@link InputGetter} that reads the values of input fields by calling
     * <code>{@link DataContext#get}("inputRecord")</code>.
     */
    private record DataContextInputGetter(AlgDataType rowType, AlgDataTypeFactory typeFactory) implements InputGetter {


        @Override
        public Expression field( BlockBuilder list, int index, Type storageType ) {
            Expression recFromCtx = Expressions.convert_( Expressions.call(
                    DataContext.ROOT,
                    BuiltInMethod.DATA_CONTEXT_GET.method,
                    Expressions.constant( "inputRecord" ) ), PolyValue[].class );
            Expression recFromCtxCasted = RexToLixTranslator.convert( recFromCtx, PolyValue[].class );
            IndexExpression recordAccess = Expressions.arrayIndex( recFromCtxCasted, Expressions.constant( index ) );
            if ( storageType == null ) {
                final AlgDataType fieldType = rowType.getFields().get( index ).getType();
                storageType = ((JavaTypeFactory) typeFactory).getJavaClass( fieldType );
            }
            return RexToLixTranslator.convert( recordAccess, storageType );
        }

    }

}

