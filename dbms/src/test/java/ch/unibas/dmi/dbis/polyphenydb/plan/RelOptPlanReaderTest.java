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

package ch.unibas.dmi.dbis.polyphenydb.plan;


import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.CoreMatchers.sameInstance;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.fail;

import ch.unibas.dmi.dbis.polyphenydb.adapter.jdbc.JdbcRules;
import ch.unibas.dmi.dbis.polyphenydb.rel.AbstractRelNode;
import ch.unibas.dmi.dbis.polyphenydb.rel.externalize.RelJson;
import ch.unibas.dmi.dbis.polyphenydb.rel.logical.LogicalProject;
import org.junit.Test;


/**
 * Unit test for {@link ch.unibas.dmi.dbis.polyphenydb.rel.externalize.RelJson}.
 */
public class RelOptPlanReaderTest {

    @Test
    public void testTypeToClass() {
        RelJson relJson = new RelJson( null );

        // in ch.unibas.dmi.dbis.polyphenydb.rel package
        assertThat( relJson.classToTypeName( LogicalProject.class ), is( "LogicalProject" ) );
        assertThat( relJson.typeNameToClass( "LogicalProject" ), sameInstance( LogicalProject.class ) );

        // in ch.unibas.dmi.dbis.polyphenydb.adapter.jdbc.JdbcRules outer class
        assertThat( relJson.classToTypeName( JdbcRules.JdbcProject.class ), is( "JdbcProject" ) );
        assertThat( relJson.typeNameToClass( "JdbcProject" ), equalTo( JdbcRules.JdbcProject.class ) );

        try {
            Class clazz = relJson.typeNameToClass( "NonExistentRel" );
            fail( "expected exception, got " + clazz );
        } catch ( RuntimeException e ) {
            assertThat( e.getMessage(), is( "unknown type NonExistentRel" ) );
        }
        try {
            Class clazz = relJson.typeNameToClass( "ch.unibas.dmi.dbis.polyphenydb.rel.NonExistentRel" );
            fail( "expected exception, got " + clazz );
        } catch ( RuntimeException e ) {
            assertThat( e.getMessage(), is( "unknown type ch.unibas.dmi.dbis.polyphenydb.rel.NonExistentRel" ) );
        }

        // In this class; no special treatment. Note: '$MyRel' not '.MyRel'.
        assertThat( relJson.classToTypeName( MyRel.class ), is( "ch.unibas.dmi.dbis.polyphenydb.plan.RelOptPlanReaderTest$MyRel" ) );
        assertThat( relJson.typeNameToClass( MyRel.class.getName() ), equalTo( MyRel.class ) );

        // Using canonical name (with '$'), not found
        try {
            Class clazz = relJson.typeNameToClass( MyRel.class.getCanonicalName() );
            fail( "expected exception, got " + clazz );
        } catch ( RuntimeException e ) {
            assertThat( e.getMessage(), is( "unknown type ch.unibas.dmi.dbis.polyphenydb.plan.RelOptPlanReaderTest.MyRel" ) );
        }
    }


    /**
     * Dummy relational expression.
     */
    public static class MyRel extends AbstractRelNode {

        public MyRel( RelOptCluster cluster, RelTraitSet traitSet ) {
            super( cluster, traitSet );
        }
    }
}

