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


import ch.unibas.dmi.dbis.polyphenydb.config.RuntimeConfig;
import ch.unibas.dmi.dbis.polyphenydb.prepare.Prepare;
import ch.unibas.dmi.dbis.polyphenydb.util.TryThreadLocal;
import java.util.Collection;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;


/**
 * Test that runs every Quidem file in the "core" module as a test.
 */
@RunWith(Parameterized.class)
public class CoreQuidemTest extends QuidemTest {

    public CoreQuidemTest( String path ) {
        super( path );
    }


    /**
     * Runs a test from the command line.
     *
     * For example:
     * <code>java CoreQuidemTest sql/dummy.iq</code>
     */
    public static void main( String[] args ) throws Exception {
        for ( String arg : args ) {
            new CoreQuidemTest( arg ).test();
        }
    }


    /**
     * For {@link Parameterized} runner.
     */
    @Parameterized.Parameters(name = "{index}: quidem({0})")
    public static Collection<Object[]> data() {
        // Start with a test file we know exists, then find the directory and list its files.
        final String first = "sql/agg.iq";
        return data( first );
    }


    /**
     * Override settings for "sql/misc.iq".
     */
    public void testSqlMisc() throws Exception {
        switch ( PolyphenyDbAssert.DB ) {
            case ORACLE:
                // There are formatting differences (e.g. "4.000" vs "4") when using Oracle as the JDBC data source.
                return;
        }
        final boolean oldCaseSensitiveValue = RuntimeConfig.CASE_SENSITIVE.getBoolean();
        try ( TryThreadLocal.Memo ignored = Prepare.THREAD_EXPAND.push( true ) ) {
            RuntimeConfig.CASE_SENSITIVE.setBoolean( true );
            checkRun( path );
        } finally {
            RuntimeConfig.CASE_SENSITIVE.setBoolean( oldCaseSensitiveValue );
        }
    }


    /**
     * Override settings for "sql/scalar.iq".
     */
    public void testSqlScalar() throws Exception {
        try ( TryThreadLocal.Memo ignored = Prepare.THREAD_EXPAND.push( true ) ) {
            checkRun( path );
        }
    }


    /**
     * Runs the dummy script "sql/dummy.iq", which is checked in empty but which you may use as scratch space during development.
     */

    // Do not disable this test; just remember not to commit changes to dummy.iq
    public void testSqlDummy() throws Exception {
        try ( TryThreadLocal.Memo ignored = Prepare.THREAD_EXPAND.push( true ) ) {
            checkRun( path );
        }
    }

}

