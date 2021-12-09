/*
 * Copyright 2019-2021 The Polypheny Project
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

package org.polypheny.db.adapter.druid;


import static org.hamcrest.core.Is.is;

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
import org.polypheny.db.algebra.operators.OperatorName;
import org.polypheny.db.algebra.type.AlgDataType;
import org.polypheny.db.algebra.type.AlgDataTypeSystem;
import org.polypheny.db.config.PolyphenyDbConnectionConfig;
import org.polypheny.db.languages.OperatorRegistry;
import org.polypheny.db.prepare.JavaTypeFactoryImpl;
import org.polypheny.db.rex.RexBuilder;
import org.polypheny.db.rex.RexNode;
import org.polypheny.db.type.PolyType;


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

        RexNode inRexNode = f.rexBuilder.makeCall( OperatorRegistry.get( OperatorName.IN ), listRexNodes );
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
        AlgDataType algDataType = f.typeFactory.createPolyType( PolyType.BOOLEAN );
        RexNode betweenRexNode = f.rexBuilder.makeCall( algDataType, OperatorRegistry.get( OperatorName.BETWEEN ), listRexNodes );

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

        final JavaTypeFactoryImpl typeFactory = new JavaTypeFactoryImpl( AlgDataTypeSystem.DEFAULT );
        final RexBuilder rexBuilder = new RexBuilder( typeFactory );
        final DruidTable druidTable = new DruidTable( Mockito.mock( DruidSchema.class ), "dataSource", null, ImmutableSet.of(), "timestamp", null, null, null );
        final AlgDataType varcharType = typeFactory.createPolyType( PolyType.VARCHAR );
        final AlgDataType varcharRowType = typeFactory.builder().add( "dimensionName", null, varcharType ).build();

    }

}
