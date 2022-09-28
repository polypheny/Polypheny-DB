/*
 * Copyright 2019-2022 The Polypheny Project
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


import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.CoreMatchers.is;
import static org.junit.Assert.assertThat;

import org.junit.Test;
import org.polypheny.db.languages.NodeParseException;
import org.polypheny.db.languages.Parser;
import org.polypheny.db.nodes.Node;
import org.polypheny.db.sql.language.SqlCall;
import org.polypheny.db.sql.language.SqlNode;
import org.polypheny.db.sql.language.SqlSetOption;


/**
 * Test for {@link SqlSetOption}.
 */
public class SqlSetOptionOperatorTest {

    @Test
    public void testSqlSetOptionOperatorScopeSet() throws NodeParseException {
        Node node = parse( "alter system set optionA.optionB.optionC = true" );
        checkSqlSetOptionSame( node );
    }


    public Node parse( String s ) throws NodeParseException {
        return Parser.create( s ).parseStmt();
    }


    @Test
    public void testSqlSetOptionOperatorScopeReset() throws NodeParseException {
        Node node = parse( "alter session reset param1.param2.param3" );
        checkSqlSetOptionSame( node );
    }


    private static void checkSqlSetOptionSame( Node node ) {
        SqlSetOption opt = (SqlSetOption) node;
        SqlNode[] sqlNodes = new SqlNode[opt.getOperandList().size()];
        SqlCall returned = (SqlCall) opt.getOperator().createCall( opt.getFunctionQuantifier(), opt.getPos(), opt.getOperandList().toArray( sqlNodes ) );
        assertThat( opt.getClass(), equalTo( (Class) returned.getClass() ) );
        SqlSetOption optRet = (SqlSetOption) returned;
        assertThat( optRet.getScope(), is( opt.getScope() ) );
        assertThat( optRet.getName(), is( opt.getName() ) );
        assertThat( optRet.getFunctionQuantifier(), is( opt.getFunctionQuantifier() ) );
        assertThat( optRet.getPos(), is( opt.getPos() ) );
        assertThat( optRet.getValue(), is( opt.getValue() ) );
        assertThat( optRet.toString(), is( opt.toString() ) );
    }

}

