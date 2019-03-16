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
 *  The MIT License (MIT)
 *
 *  Copyright (c) 2019 Databases and Information Systems Research Group, University of Basel, Switzerland
 *
 *  Permission is hereby granted, free of charge, to any person obtaining a
 *  copy of this software and associated documentation files (the "Software"), to deal
 *  in the Software without restriction, including without limitation the rights
 *  to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
 *  copies of the Software, and to permit persons to whom the Software is
 *  furnished to do so, subject to the following conditions:
 *
 *  The above copyright notice and this permission notice shall be included in all
 *  copies or substantial portions of the Software.
 *
 *  THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
 *  IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
 *  FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
 *  AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
 *  LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
 *  OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
 *  SOFTWARE.
 */

package ch.unibas.dmi.dbis.polyphenydb.test;


import ch.unibas.dmi.dbis.polyphenydb.rel.type.RelDataTypeFactory;
import ch.unibas.dmi.dbis.polyphenydb.runtime.PolyphenyDbContextException;
import ch.unibas.dmi.dbis.polyphenydb.runtime.PolyphenyDbException;
import ch.unibas.dmi.dbis.polyphenydb.runtime.Feature;
import ch.unibas.dmi.dbis.polyphenydb.sql.SqlOperatorTable;
import ch.unibas.dmi.dbis.polyphenydb.sql.parser.SqlParserPos;
import ch.unibas.dmi.dbis.polyphenydb.sql.test.SqlTestFactory;
import ch.unibas.dmi.dbis.polyphenydb.sql.test.SqlTester;
import ch.unibas.dmi.dbis.polyphenydb.sql.test.SqlValidatorTester;
import ch.unibas.dmi.dbis.polyphenydb.sql.validate.SqlConformance;
import ch.unibas.dmi.dbis.polyphenydb.sql.validate.SqlValidatorCatalogReader;
import ch.unibas.dmi.dbis.polyphenydb.sql.validate.SqlValidatorImpl;
import ch.unibas.dmi.dbis.polyphenydb.util.Static;
import org.junit.Test;


/**
 * SqlValidatorFeatureTest verifies that features can be independently enabled or disabled.
 */
public class SqlValidatorFeatureTest extends SqlValidatorTestCase {

    private static final String FEATURE_DISABLED = "feature_disabled";


    private Feature disabledFeature;


    public SqlValidatorFeatureTest() {
        super();
    }


    @Override
    public SqlTester getTester() {
        return new SqlValidatorTester( SqlTestFactory.INSTANCE.withValidator( FeatureValidator::new ) );
    }


    @Test
    public void testDistinct() {
        checkFeature( "select ^distinct^ name from dept", Static.RESOURCE.sQLFeature_E051_01() );
    }


    @Test
    public void testOrderByDesc() {
        checkFeature( "select name from dept order by ^name desc^", Static.RESOURCE.sQLConformance_OrderByDesc() );
    }

    // NOTE jvs 6-Mar-2006:  carets don't come out properly placed for INTERSECT/EXCEPT, so don't bother


    @Test
    public void testIntersect() {
        checkFeature( "^select name from dept intersect select name from dept^", Static.RESOURCE.sQLFeature_F302() );
    }


    @Test
    public void testExcept() {
        checkFeature( "^select name from dept except select name from dept^", Static.RESOURCE.sQLFeature_E071_03() );
    }


    @Test
    public void testMultiset() {
        checkFeature( "values ^multiset[1]^", Static.RESOURCE.sQLFeature_S271() );
        checkFeature( "values ^multiset(select * from dept)^", Static.RESOURCE.sQLFeature_S271() );
    }


    @Test
    public void testTablesample() {
        checkFeature( "select name from ^dept tablesample bernoulli(50)^", Static.RESOURCE.sQLFeature_T613() );
        checkFeature( "select name from ^dept tablesample substitute('sample_dept')^", Static.RESOURCE.sQLFeatureExt_T613_Substitution() );
    }


    private void checkFeature( String sql, Feature feature ) {
        // Test once with feature enabled:  should pass
        check( sql );

        // Test once with feature disabled:  should fail
        try {
            disabledFeature = feature;
            checkFails( sql, FEATURE_DISABLED );
        } finally {
            disabledFeature = null;
        }
    }


    /**
     * Extension to {@link SqlValidatorImpl} that validates features.
     */
    public class FeatureValidator extends SqlValidatorImpl {

        protected FeatureValidator( SqlOperatorTable opTab, SqlValidatorCatalogReader catalogReader, RelDataTypeFactory typeFactory, SqlConformance conformance ) {
            super( opTab, catalogReader, typeFactory, conformance );
        }


        protected void validateFeature( Feature feature, SqlParserPos context ) {
            if ( feature.equals( disabledFeature ) ) {
                PolyphenyDbException ex = new PolyphenyDbException( FEATURE_DISABLED, null );
                if ( context == null ) {
                    throw ex;
                }
                throw new PolyphenyDbContextException( "location", ex, context.getLineNum(), context.getColumnNum(), context.getEndLineNum(), context.getEndColumnNum() );
            }
        }
    }
}

