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

package org.polypheny.db.sql;


import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.List;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.polypheny.db.algebra.constant.Kind;
import org.polypheny.db.algebra.operators.OperatorName;
import org.polypheny.db.algebra.type.AlgDataTypeFactory;
import org.polypheny.db.algebra.type.AlgDataTypeSystem;
import org.polypheny.db.languages.OperatorRegistry;
import org.polypheny.db.rex.RexBuilder;
import org.polypheny.db.rex.RexExecutorImpl;
import org.polypheny.db.rex.RexLiteral;
import org.polypheny.db.rex.RexNode;
import org.polypheny.db.type.PolyType;
import org.polypheny.db.type.PolyTypeFactoryImpl;
import org.polypheny.db.type.entity.PolyBoolean;
import org.polypheny.db.type.entity.numerical.PolyInteger;
import org.polypheny.db.util.PolyphenyHomeDirManager;
import org.polypheny.db.util.RunMode;

public class RexBuilderTest {

    @BeforeAll
    public static void init() {
        if ( PolyphenyHomeDirManager.getMode() == null ) {
            PolyphenyHomeDirManager.setModeAndGetInstance( RunMode.TEST );
        }
        if ( !SqlLanguagePlugin.isInit() ) {
            SqlLanguagePlugin.startup();
        }
    }


    /**
     * Test RexBuilder.ensureType()
     */
    @Test
    public void testEnsureTypeWithDifference() {
        final AlgDataTypeFactory typeFactory = new PolyTypeFactoryImpl( AlgDataTypeSystem.DEFAULT );
        RexBuilder builder = new RexBuilder( typeFactory );

        RexNode node = new RexLiteral( PolyBoolean.TRUE, typeFactory.createPolyType( PolyType.BOOLEAN ), PolyType.BOOLEAN );
        RexNode ensuredNode = builder.ensureType( typeFactory.createPolyType( PolyType.INTEGER ), node, true );

        assertNotEquals( node, ensuredNode );
        assertEquals( ensuredNode.getType(), typeFactory.createPolyType( PolyType.INTEGER ) );
    }


    @Test
    public void mappingConvertShowsTargetTypeInPlan() {
        var typeFactory = new PolyTypeFactoryImpl( AlgDataTypeSystem.DEFAULT );
        var builder = new RexBuilder( typeFactory );
        var input = builder.makeInputRef( typeFactory.createPolyType( PolyType.VARCHAR ), 0 );
        var fieldName = builder.makeLiteral( "amount" );
        var operator = OperatorRegistry.get( OperatorName.MAPPING_CONVERT );
        var integer = typeFactory.createPolyType( PolyType.INTEGER );
        var nullableInteger = typeFactory.createTypeWithNullability( integer, true );
        var conversion = builder.makeCall( integer, operator, input, fieldName );
        var nullableConversion = builder.makeCall( nullableInteger, operator, input, fieldName );

        assertEquals( Kind.MAPPING_CONVERT, conversion.getKind() );
        assertTrue( conversion.toString().endsWith( ":" + integer.getFullTypeString() ) );
        assertNotEquals( conversion, nullableConversion );
    }


    @Test
    public void mappingConvertExecutesWithDedicatedKind() {
        var typeFactory = new PolyTypeFactoryImpl( AlgDataTypeSystem.DEFAULT );
        var builder = new RexBuilder( typeFactory );
        var conversion = builder.makeCall( typeFactory.createPolyType( PolyType.INTEGER ),
                OperatorRegistry.get( OperatorName.MAPPING_CONVERT ), builder.makeLiteral( "42" ), builder.makeLiteral( "amount" ) );
        var executable = new RexExecutorImpl( null ).getExecutable( builder, List.of( conversion ), typeFactory.builder().build() );

        assertEquals( PolyInteger.of( 42 ), executable.getFunction().apply( null )[0] );
    }

}
