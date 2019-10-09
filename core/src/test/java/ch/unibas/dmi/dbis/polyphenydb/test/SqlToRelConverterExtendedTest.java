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


import ch.unibas.dmi.dbis.polyphenydb.plan.RelOptSchema;
import ch.unibas.dmi.dbis.polyphenydb.rel.RelNode;
import ch.unibas.dmi.dbis.polyphenydb.rel.RelShuttleImpl;
import ch.unibas.dmi.dbis.polyphenydb.rel.core.TableScan;
import ch.unibas.dmi.dbis.polyphenydb.rel.externalize.RelJsonReader;
import ch.unibas.dmi.dbis.polyphenydb.rel.externalize.RelJsonWriter;
import ch.unibas.dmi.dbis.polyphenydb.runtime.Hook;
import ch.unibas.dmi.dbis.polyphenydb.runtime.Hook.Closeable;
import ch.unibas.dmi.dbis.polyphenydb.tools.Frameworks;
import java.io.IOException;
import org.junit.After;
import org.junit.Before;


/**
 * Runs {@link ch.unibas.dmi.dbis.polyphenydb.test.SqlToRelConverterTest} with extensions.
 */
public class SqlToRelConverterExtendedTest extends SqlToRelConverterTest {

    Closeable closeable;


    @Before
    public void before() {
        this.closeable = Hook.CONVERTED.addThread( SqlToRelConverterExtendedTest::foo );
    }


    @After
    public void after() {
        if ( this.closeable != null ) {
            this.closeable.close();
            this.closeable = null;
        }
    }


    public static void foo( RelNode rel ) {
        // Convert rel tree to JSON.
        final RelJsonWriter writer = new RelJsonWriter();
        rel.explain( writer );
        final String json = writer.asString();

        // Find the schema. If there are no tables in the plan, we won't need one.
        final RelOptSchema[] schemas = { null };
        rel.accept( new RelShuttleImpl() {
            @Override
            public RelNode visit( TableScan scan ) {
                schemas[0] = scan.getTable().getRelOptSchema();
                return super.visit( scan );
            }
        } );

        // Convert JSON back to rel tree.
        Frameworks.withPlanner( ( cluster, relOptSchema, rootSchema ) -> {
            final RelJsonReader reader = new RelJsonReader( cluster, schemas[0], rootSchema );
            try {
                RelNode x = reader.read( json );
            } catch ( IOException e ) {
                throw new RuntimeException( e );
            }
            return null;
        } );
    }
}

