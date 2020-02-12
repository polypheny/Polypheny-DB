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
 */

package ch.unibas.dmi.dbis.polyphenydb.test;


import static org.junit.Assert.assertSame;

import ch.unibas.dmi.dbis.polyphenydb.adapter.java.JavaTypeFactory;
import ch.unibas.dmi.dbis.polyphenydb.jdbc.JavaTypeFactoryImpl;
import ch.unibas.dmi.dbis.polyphenydb.rel.type.RelDataType;
import ch.unibas.dmi.dbis.polyphenydb.rel.type.RelDataTypeSystem;
import ch.unibas.dmi.dbis.polyphenydb.rex.RexBuilder;
import ch.unibas.dmi.dbis.polyphenydb.rex.RexNode;
import ch.unibas.dmi.dbis.polyphenydb.rex.RexProgram;
import ch.unibas.dmi.dbis.polyphenydb.rex.RexProgramBuilder;
import ch.unibas.dmi.dbis.polyphenydb.rex.RexUtil;
import ch.unibas.dmi.dbis.polyphenydb.sql.SqlDataTypeSpec;
import ch.unibas.dmi.dbis.polyphenydb.sql.SqlLiteral;
import ch.unibas.dmi.dbis.polyphenydb.sql.SqlNode;
import ch.unibas.dmi.dbis.polyphenydb.sql.SqlUtil;
import ch.unibas.dmi.dbis.polyphenydb.sql.fun.SqlStdOperatorTable;
import ch.unibas.dmi.dbis.polyphenydb.sql.parser.SqlParserPos;
import ch.unibas.dmi.dbis.polyphenydb.sql.type.SqlTypeName;
import ch.unibas.dmi.dbis.polyphenydb.sql.type.SqlTypeUtil;
import com.google.common.collect.Lists;
import org.junit.Before;
import org.junit.Test;


/**
 * Unit tests for {@link RexProgram} and {@link RexProgramBuilder}.
 */
public class SqlOperatorBindingTest {

    private RexBuilder rexBuilder;
    private RelDataType integerDataType;
    private SqlDataTypeSpec integerType;


    /**
     * Creates a SqlOperatorBindingTest.
     */
    public SqlOperatorBindingTest() {
        super();
    }


    @Before
    public void setUp() {
        JavaTypeFactory typeFactory = new JavaTypeFactoryImpl( RelDataTypeSystem.DEFAULT );
        integerDataType = typeFactory.createSqlType( SqlTypeName.INTEGER );
        integerType = SqlTypeUtil.convertTypeToSpec( integerDataType );
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
                SqlParserPos.ZERO );
        final SqlNode castLiteral = SqlStdOperatorTable.CAST.createCall(
                SqlParserPos.ZERO,
                literal,
                integerType );
        final SqlNode castCastLiteral = SqlStdOperatorTable.CAST.createCall(
                SqlParserPos.ZERO,
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
     * Tests {@link ch.unibas.dmi.dbis.polyphenydb.rex.RexUtil#isLiteral(RexNode, boolean)}, which was added to enhance Polypheny-DB's public API
     * "Add a method to SqlOperatorBinding to determine whether operand is a literal".
     */
    @Test
    public void testRexNodeLiteral() {
        final RexNode literal = rexBuilder.makeZeroLiteral(
                integerDataType );

        final RexNode castLiteral = rexBuilder.makeCall(
                integerDataType,
                SqlStdOperatorTable.CAST,
                Lists.newArrayList( literal ) );

        final RexNode castCastLiteral = rexBuilder.makeCall(
                integerDataType,
                SqlStdOperatorTable.CAST,
                Lists.newArrayList( castLiteral ) );

        // RexLiteral is considered as a Literal
        assertSame( true, RexUtil.isLiteral( literal, true ) );
        // CAST(RexLiteral as type) is considered as a Literal
        assertSame( true, RexUtil.isLiteral( castLiteral, true ) );
        // CAST(CAST(RexLiteral as type) as type) is NOT considered as a Literal
        assertSame( false, RexUtil.isLiteral( castCastLiteral, true ) );
    }
}

