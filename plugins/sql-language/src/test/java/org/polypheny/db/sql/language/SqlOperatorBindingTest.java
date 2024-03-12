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
 */

package org.polypheny.db.sql.language;


import static org.junit.jupiter.api.Assertions.assertSame;

import com.google.common.collect.Lists;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.polypheny.db.adapter.java.JavaTypeFactory;
import org.polypheny.db.algebra.operators.OperatorName;
import org.polypheny.db.algebra.type.AlgDataType;
import org.polypheny.db.algebra.type.AlgDataTypeSystem;
import org.polypheny.db.languages.OperatorRegistry;
import org.polypheny.db.languages.ParserPos;
import org.polypheny.db.prepare.JavaTypeFactoryImpl;
import org.polypheny.db.rex.RexBuilder;
import org.polypheny.db.rex.RexNode;
import org.polypheny.db.rex.RexProgram;
import org.polypheny.db.rex.RexProgramBuilder;
import org.polypheny.db.rex.RexUtil;
import org.polypheny.db.sql.language.util.SqlTypeUtil;
import org.polypheny.db.type.PolyType;


/**
 * Unit tests for {@link RexProgram} and {@link RexProgramBuilder}.
 */
public class SqlOperatorBindingTest {

    private RexBuilder rexBuilder;
    private AlgDataType integerDataType;
    private SqlDataTypeSpec integerType;


    /**
     * Creates a SqlOperatorBindingTest.
     */
    public SqlOperatorBindingTest() {
        super();
    }


    @BeforeEach
    public void setUp() {
        JavaTypeFactory typeFactory = new JavaTypeFactoryImpl( AlgDataTypeSystem.DEFAULT );
        integerDataType = typeFactory.createPolyType( PolyType.INTEGER );
        integerType = (SqlDataTypeSpec) SqlTypeUtil.convertTypeToSpec( integerDataType );
        rexBuilder = new RexBuilder( typeFactory );
    }


    /**
     * Tests {@link SqlUtil#isLiteral(SqlNode, boolean)}, which was added to enhance Polypheny-DB's public API
     * "Add a method to SqlOperatorBinding to determine whether operand is a literal".
     */
    @Test
    public void testSqlNodeLiteral() {
        final SqlNode literal = SqlLiteral.createExactNumeric(
                "0",
                ParserPos.ZERO );
        final SqlNode castLiteral = (SqlNode) OperatorRegistry.get( OperatorName.CAST ).createCall(
                ParserPos.ZERO,
                literal,
                integerType );
        final SqlNode castCastLiteral = (SqlNode) OperatorRegistry.get( OperatorName.CAST ).createCall(
                ParserPos.ZERO,
                castLiteral,
                integerType );

        // SqlLiteral is considered as a Literal
        assertSame( true, SqlUtil.isLiteral( literal, true ) );
        // CAST(SqlLiteral as type) is considered as a Literal
        assertSame( true, SqlUtil.isLiteral( castLiteral, true ) );
        // CAST(CAST(SqlLiteral as type) as type) is NOT considered as a Literal
        assertSame( false, SqlUtil.isLiteral( castCastLiteral, true ) );
    }


    /**
     * Tests {@link org.polypheny.db.rex.RexUtil#isLiteral(RexNode, boolean)}, which was added to enhance Polypheny-DB's public API
     * "Add a method to SqlOperatorBinding to determine whether operand is a literal".
     */
    @Test
    public void testRexNodeLiteral() {
        final RexNode literal = rexBuilder.makeZeroLiteral(
                integerDataType );

        final RexNode castLiteral = rexBuilder.makeCall(
                integerDataType,
                OperatorRegistry.get( OperatorName.CAST ),
                Lists.newArrayList( literal ) );

        final RexNode castCastLiteral = rexBuilder.makeCall(
                integerDataType,
                OperatorRegistry.get( OperatorName.CAST ),
                Lists.newArrayList( castLiteral ) );
        // RexLiteral is considered as a Literal
        assertSame( true, RexUtil.isLiteral( literal, true ) );
        // CAST(RexLiteral as type) is considered as a Literal
        assertSame( true, RexUtil.isLiteral( castLiteral, true ) );
        // CAST(CAST(RexLiteral as type) as type) is NOT considered as a Literal
        assertSame( false, RexUtil.isLiteral( castCastLiteral, true ) );
    }

}

