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
 */

package org.polypheny.db.languages.sql;


import java.io.IOException;
import org.junit.After;
import org.junit.Before;
import org.polypheny.db.plan.RelOptSchema;
import org.polypheny.db.rel.RelNode;
import org.polypheny.db.rel.RelShuttleImpl;
import org.polypheny.db.rel.core.TableScan;
import org.polypheny.db.rel.externalize.RelJsonReader;
import org.polypheny.db.rel.externalize.RelJsonWriter;
import org.polypheny.db.runtime.Hook;
import org.polypheny.db.runtime.Hook.Closeable;
import org.polypheny.db.tools.Frameworks;


/**
 * Runs {@link SqlToRelConverterTest} with extensions.
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

