/*
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
 *
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

