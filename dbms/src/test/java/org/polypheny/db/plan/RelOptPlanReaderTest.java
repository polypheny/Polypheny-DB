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

package org.polypheny.db.plan;


import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.CoreMatchers.sameInstance;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.fail;

import org.junit.Test;
import org.polypheny.db.adapter.jdbc.JdbcRules;
import org.polypheny.db.algebra.AbstractAlgNode;
import org.polypheny.db.algebra.externalize.AlgJson;
import org.polypheny.db.algebra.logical.relational.LogicalProject;


/**
 * Unit test for {@link AlgJson}.
 */
public class RelOptPlanReaderTest {

    @Test
    public void testTypeToClass() {
        AlgJson algJson = new AlgJson( null );

        // in org.polypheny.db.alg package
        assertThat( algJson.classToTypeName( LogicalProject.class ), is( "LogicalProject" ) );
        assertThat( algJson.typeNameToClass( "LogicalProject" ), sameInstance( LogicalProject.class ) );

        // in org.polypheny.db.adapter.jdbc.JdbcRules outer class
        assertThat( algJson.classToTypeName( JdbcRules.JdbcProject.class ), is( "JdbcProject" ) );
        assertThat( algJson.typeNameToClass( "JdbcProject" ), equalTo( JdbcRules.JdbcProject.class ) );

        try {
            Class clazz = algJson.typeNameToClass( "NonExistentRel" );
            fail( "expected exception, got " + clazz );
        } catch ( RuntimeException e ) {
            assertThat( e.getMessage(), is( "unknown type NonExistentRel" ) );
        }
        try {
            Class clazz = algJson.typeNameToClass( "org.polypheny.db.alg.NonExistentRel" );
            fail( "expected exception, got " + clazz );
        } catch ( RuntimeException e ) {
            assertThat( e.getMessage(), is( "unknown type org.polypheny.db.alg.NonExistentRel" ) );
        }

        // In this class; no special treatment. Note: '$MyRel' not '.MyRel'.
        assertThat( algJson.classToTypeName( MyRel.class ), is( "org.polypheny.db.plan.RelOptPlanReaderTest$MyRel" ) );
        assertThat( algJson.typeNameToClass( MyRel.class.getName() ), equalTo( MyRel.class ) );

        // Using canonical name (with '$'), not found
        try {
            Class clazz = algJson.typeNameToClass( MyRel.class.getCanonicalName() );
            fail( "expected exception, got " + clazz );
        } catch ( RuntimeException e ) {
            assertThat( e.getMessage(), is( "unknown type org.polypheny.db.plan.RelOptPlanReaderTest.MyRel" ) );
        }
    }


    /**
     * Dummy relational expression.
     */
    public static class MyRel extends AbstractAlgNode {

        public MyRel( AlgOptCluster cluster, AlgTraitSet traitSet ) {
            super( cluster, traitSet );
        }


        @Override
        public String algCompareString() {
            // Compare makes no sense here. Use hashCode() to avoid errors.
            return this.getClass().getSimpleName() + "$" + hashCode() + "&";
        }

    }

}

