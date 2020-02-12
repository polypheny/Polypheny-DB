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

package ch.unibas.dmi.dbis.polyphenydb.rex;


import static org.junit.Assert.assertEquals;

import ch.unibas.dmi.dbis.polyphenydb.DataContext.SlimDataContext;
import ch.unibas.dmi.dbis.polyphenydb.adapter.java.JavaTypeFactory;
import ch.unibas.dmi.dbis.polyphenydb.jdbc.ContextImpl;
import ch.unibas.dmi.dbis.polyphenydb.jdbc.JavaTypeFactoryImpl;
import ch.unibas.dmi.dbis.polyphenydb.rel.RelNode;
import ch.unibas.dmi.dbis.polyphenydb.rel.core.Project;
import ch.unibas.dmi.dbis.polyphenydb.runtime.Hook;
import ch.unibas.dmi.dbis.polyphenydb.schema.AbstractPolyphenyDbSchema;
import ch.unibas.dmi.dbis.polyphenydb.schema.PolyphenyDbSchema;
import ch.unibas.dmi.dbis.polyphenydb.sql.SqlNode;
import ch.unibas.dmi.dbis.polyphenydb.sql.parser.SqlParseException;
import ch.unibas.dmi.dbis.polyphenydb.sql.parser.SqlParser;
import ch.unibas.dmi.dbis.polyphenydb.test.SqlToRelTestBase;
import ch.unibas.dmi.dbis.polyphenydb.tools.FrameworkConfig;
import ch.unibas.dmi.dbis.polyphenydb.tools.Frameworks;
import ch.unibas.dmi.dbis.polyphenydb.tools.Planner;
import ch.unibas.dmi.dbis.polyphenydb.tools.RelConversionException;
import ch.unibas.dmi.dbis.polyphenydb.tools.ValidationException;
import ch.unibas.dmi.dbis.polyphenydb.util.Closer;
import org.junit.Test;


/**
 * Unit test for {@link ch.unibas.dmi.dbis.polyphenydb.rex.RexSqlStandardConvertletTable}.
 */
public class RexSqlStandardConvertletTableTest extends SqlToRelTestBase {

    @Test
    public void testCoalesce() {
        final Project project = (Project) convertSqlToRel( "SELECT COALESCE(NULL, 'a')", false );
        final RexNode rex = project.getChildExps().get( 0 );
        final RexToSqlNodeConverter rexToSqlNodeConverter = rexToSqlNodeConverter();
        final SqlNode convertedSql = rexToSqlNodeConverter.convertNode( rex );
        assertEquals( "CASE WHEN NULL IS NOT NULL THEN NULL ELSE 'a' END", convertedSql.toString() );
    }


    @Test
    public void testCaseWithValue() {
        final Project project = (Project) convertSqlToRel( "SELECT CASE NULL WHEN NULL THEN NULL ELSE 'a' END", false );
        final RexNode rex = project.getChildExps().get( 0 );
        final RexToSqlNodeConverter rexToSqlNodeConverter = rexToSqlNodeConverter();
        final SqlNode convertedSql = rexToSqlNodeConverter.convertNode( rex );
        assertEquals( "CASE WHEN NULL = NULL THEN NULL ELSE 'a' END", convertedSql.toString() );
    }


    @Test
    public void testCaseNoValue() {
        final Project project = (Project) convertSqlToRel( "SELECT CASE WHEN NULL IS NULL THEN NULL ELSE 'a' END", false );
        final RexNode rex = project.getChildExps().get( 0 );
        final RexToSqlNodeConverter rexToSqlNodeConverter = rexToSqlNodeConverter();
        final SqlNode convertedSql = rexToSqlNodeConverter.convertNode( rex );
        assertEquals( "CASE WHEN NULL IS NULL THEN NULL ELSE 'a' END", convertedSql.toString() );
    }


    private RelNode convertSqlToRel( String sql, boolean simplifyRex ) {
        PolyphenyDbSchema rootSchema = AbstractPolyphenyDbSchema.createRootSchema( false );
        final FrameworkConfig config = Frameworks.newConfigBuilder()
                .defaultSchema( rootSchema.plus() )
                .parserConfig( SqlParser.configBuilder().build() )
                .prepareContext( new ContextImpl(
                        rootSchema,
                        new SlimDataContext() {
                            @Override
                            public JavaTypeFactory getTypeFactory() {
                                return new JavaTypeFactoryImpl();
                            }
                        },
                        "",
                        0,
                        0,
                        null ) )
                .build();
        final Planner planner = Frameworks.getPlanner( config );
        try ( Closer closer = new Closer() ) {
            closer.add( Hook.REL_BUILDER_SIMPLIFY.addThread( Hook.propertyJ( simplifyRex ) ) );
            final SqlNode parsed = planner.parse( sql );
            final SqlNode validated = planner.validate( parsed );
            return planner.rel( validated ).rel;
        } catch ( SqlParseException | RelConversionException | ValidationException e ) {
            throw new RuntimeException( e );
        }
    }


    private static RexToSqlNodeConverter rexToSqlNodeConverter() {
        final RexSqlStandardConvertletTable convertletTable = new RexSqlStandardConvertletTable();
        return new RexToSqlNodeConverterImpl( convertletTable );
    }

}
