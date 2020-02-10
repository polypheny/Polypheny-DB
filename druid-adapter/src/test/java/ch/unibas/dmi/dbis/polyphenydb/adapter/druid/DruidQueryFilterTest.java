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

package ch.unibas.dmi.dbis.polyphenydb.adapter.druid;


import static org.hamcrest.core.Is.is;

import ch.unibas.dmi.dbis.polyphenydb.config.PolyphenyDbConnectionConfig;
import ch.unibas.dmi.dbis.polyphenydb.jdbc.JavaTypeFactoryImpl;
import ch.unibas.dmi.dbis.polyphenydb.rel.type.RelDataType;
import ch.unibas.dmi.dbis.polyphenydb.rel.type.RelDataTypeSystem;
import ch.unibas.dmi.dbis.polyphenydb.rex.RexBuilder;
import ch.unibas.dmi.dbis.polyphenydb.rex.RexNode;
import ch.unibas.dmi.dbis.polyphenydb.sql.fun.SqlStdOperatorTable;
import ch.unibas.dmi.dbis.polyphenydb.sql.type.SqlTypeName;
import com.fasterxml.jackson.core.JsonFactory;
import com.fasterxml.jackson.core.JsonGenerator;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import java.io.IOException;
import java.io.StringWriter;
import java.math.BigDecimal;
import java.util.List;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mockito;


/**
 * Tests generating Druid filters.
 */
public class DruidQueryFilterTest {

    private DruidQuery druidQuery;


    @Before
    public void testSetup() {
        druidQuery = Mockito.mock( DruidQuery.class );
        final PolyphenyDbConnectionConfig connectionConfigMock = Mockito.mock( PolyphenyDbConnectionConfig.class );
        Mockito.when( connectionConfigMock.timeZone() ).thenReturn( "UTC" );
        Mockito.when( druidQuery.getConnectionConfig() ).thenReturn( connectionConfigMock );
        Mockito.when( druidQuery.getDruidTable() ).thenReturn( new DruidTable( Mockito.mock( DruidSchema.class ), "dataSource", null, ImmutableSet.of(), "timestamp", null, null, null ) );
    }


    @Test
    public void testInFilter() throws IOException {
        final Fixture f = new Fixture();
        final List<? extends RexNode> listRexNodes = ImmutableList.of( f.rexBuilder.makeInputRef( f.varcharRowType, 0 ), f.rexBuilder.makeExactLiteral( BigDecimal.valueOf( 1 ) ), f.rexBuilder.makeExactLiteral( BigDecimal.valueOf( 5 ) ), f.rexBuilder.makeLiteral( "value1" ) );

        RexNode inRexNode = f.rexBuilder.makeCall( SqlStdOperatorTable.IN, listRexNodes );
        DruidJsonFilter returnValue = DruidJsonFilter.toDruidFilters( inRexNode, f.varcharRowType, druidQuery );
        Assert.assertNotNull( "Filter is null", returnValue );
        JsonFactory jsonFactory = new JsonFactory();
        final StringWriter sw = new StringWriter();
        JsonGenerator jsonGenerator = jsonFactory.createGenerator( sw );
        returnValue.write( jsonGenerator );
        jsonGenerator.close();

        Assert.assertThat( sw.toString(), is( "{\"type\":\"in\",\"dimension\":\"dimensionName\",\"values\":[\"1\",\"5\",\"value1\"]}" ) );
    }


    @Test
    public void testBetweenFilterStringCase() throws IOException {
        final Fixture f = new Fixture();
        final List<RexNode> listRexNodes = ImmutableList.of( f.rexBuilder.makeLiteral( false ), f.rexBuilder.makeInputRef( f.varcharRowType, 0 ), f.rexBuilder.makeLiteral( "lower-bound" ), f.rexBuilder.makeLiteral( "upper-bound" ) );
        RelDataType relDataType = f.typeFactory.createSqlType( SqlTypeName.BOOLEAN );
        RexNode betweenRexNode = f.rexBuilder.makeCall( relDataType, SqlStdOperatorTable.BETWEEN, listRexNodes );

        DruidJsonFilter returnValue = DruidJsonFilter.toDruidFilters( betweenRexNode, f.varcharRowType, druidQuery );
        Assert.assertNotNull( "Filter is null", returnValue );
        JsonFactory jsonFactory = new JsonFactory();
        final StringWriter sw = new StringWriter();
        JsonGenerator jsonGenerator = jsonFactory.createGenerator( sw );
        returnValue.write( jsonGenerator );
        jsonGenerator.close();
        Assert.assertThat( sw.toString(), is( "{\"type\":\"bound\",\"dimension\":\"dimensionName\",\"lower\":\"lower-bound\",\"lowerStrict\":false,\"upper\":\"upper-bound\",\"upperStrict\":false,\"ordering\":\"lexicographic\"}" ) );
    }


    /**
     * Everything a test needs for a healthy, active life.
     */
    static class Fixture {

        final JavaTypeFactoryImpl typeFactory = new JavaTypeFactoryImpl( RelDataTypeSystem.DEFAULT );
        final RexBuilder rexBuilder = new RexBuilder( typeFactory );
        final DruidTable druidTable = new DruidTable( Mockito.mock( DruidSchema.class ), "dataSource", null, ImmutableSet.of(), "timestamp", null, null, null );
        final RelDataType varcharType = typeFactory.createSqlType( SqlTypeName.VARCHAR );
        final RelDataType varcharRowType = typeFactory.builder().add( "dimensionName", null, varcharType ).build();
    }
}
