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

package ch.unibas.dmi.dbis.polyphenydb.rel.rel2sql;


import ch.unibas.dmi.dbis.polyphenydb.config.PolyphenyDbConnectionConfig;
import ch.unibas.dmi.dbis.polyphenydb.jdbc.PolyphenyDbSchema;
import ch.unibas.dmi.dbis.polyphenydb.rel.RelCollation;
import ch.unibas.dmi.dbis.polyphenydb.rel.RelDistribution;
import ch.unibas.dmi.dbis.polyphenydb.rel.RelReferentialConstraint;
import ch.unibas.dmi.dbis.polyphenydb.rel.type.RelDataType;
import ch.unibas.dmi.dbis.polyphenydb.rel.type.RelDataTypeFactory;
import ch.unibas.dmi.dbis.polyphenydb.rel.type.RelProtoDataType;
import ch.unibas.dmi.dbis.polyphenydb.schema.Function;
import ch.unibas.dmi.dbis.polyphenydb.schema.Schema;
import ch.unibas.dmi.dbis.polyphenydb.schema.SchemaPlus;
import ch.unibas.dmi.dbis.polyphenydb.schema.SchemaVersion;
import ch.unibas.dmi.dbis.polyphenydb.schema.Statistic;
import ch.unibas.dmi.dbis.polyphenydb.schema.Table;
import ch.unibas.dmi.dbis.polyphenydb.sql.SqlCall;
import ch.unibas.dmi.dbis.polyphenydb.sql.SqlNode;
import ch.unibas.dmi.dbis.polyphenydb.sql.dialect.PolyphenyDbSqlDialect;
import ch.unibas.dmi.dbis.polyphenydb.sql.type.SqlTypeName;
import ch.unibas.dmi.dbis.polyphenydb.util.ImmutableBitSet;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import java.util.Collection;
import java.util.List;
import java.util.Set;
import org.apache.calcite.linq4j.tree.Expression;
import org.junit.Test;


/**
 * Tests for {@link RelToSqlConverter} on a schema that has nested structures of multiple levels.
 */
public class RelToSqlConverterStructsTest {

    private static final Schema SCHEMA = new Schema() {
        @Override
        public Table getTable( String name ) {
            return TABLE;
        }


        @Override
        public Set<String> getTableNames() {
            return ImmutableSet.of( "myTable" );
        }


        @Override
        public RelProtoDataType getType( String name ) {
            return null;
        }


        @Override
        public Set<String> getTypeNames() {
            return ImmutableSet.of();
        }


        @Override
        public Collection<Function> getFunctions( String name ) {
            return null;
        }


        @Override
        public Set<String> getFunctionNames() {
            return ImmutableSet.of();
        }


        @Override
        public Schema getSubSchema( String name ) {
            return null;
        }


        @Override
        public Set<String> getSubSchemaNames() {
            return ImmutableSet.of();
        }


        @Override
        public Expression getExpression( SchemaPlus parentSchema, String name ) {
            return null;
        }


        @Override
        public boolean isMutable() {
            return false;
        }


        @Override
        public Schema snapshot( SchemaVersion version ) {
            return null;
        }
    };

    // Table schema is as following:
    // { a: INT, n1: { n11: { b INT }, n12: {c: Int } }, n2: { d: Int }, e: Int }
    private static final Table TABLE = new Table() {
        @Override
        public RelDataType getRowType( RelDataTypeFactory typeFactory ) {
            final RelDataType aType = typeFactory.createSqlType( SqlTypeName.BIGINT );
            final RelDataType bType = typeFactory.createSqlType( SqlTypeName.BIGINT );
            final RelDataType cType = typeFactory.createSqlType( SqlTypeName.BIGINT );
            final RelDataType dType = typeFactory.createSqlType( SqlTypeName.BIGINT );
            final RelDataType eType = typeFactory.createSqlType( SqlTypeName.BIGINT );
            final RelDataType n11Type = typeFactory.createStructType( ImmutableList.of( bType ), ImmutableList.of( "b" ) );
            final RelDataType n12Type = typeFactory.createStructType( ImmutableList.of( cType ), ImmutableList.of( "c" ) );
            final RelDataType n1Type = typeFactory.createStructType( ImmutableList.of( n11Type, n12Type ), ImmutableList.of( "n11", "n12" ) );
            final RelDataType n2Type = typeFactory.createStructType( ImmutableList.of( dType ), ImmutableList.of( "d" ) );
            return typeFactory.createStructType(
                    ImmutableList.of( aType, n1Type, n2Type, eType ),
                    ImmutableList.of( "a", "n1", "n2", "e" ) );
        }


        @Override
        public Statistic getStatistic() {
            return STATS;
        }


        @Override
        public Schema.TableType getJdbcTableType() {
            return null;
        }


        @Override
        public boolean isRolledUp( String column ) {
            return false;
        }


        @Override
        public boolean rolledUpColumnValidInsideAgg( String column, SqlCall call, SqlNode parent, PolyphenyDbConnectionConfig config ) {
            return false;
        }
    };

    private static final Statistic STATS = new Statistic() {
        @Override
        public Double getRowCount() {
            return 0D;
        }


        @Override
        public boolean isKey( ImmutableBitSet columns ) {
            return false;
        }


        @Override
        public List<RelReferentialConstraint> getReferentialConstraints() {
            return ImmutableList.of();
        }


        @Override
        public List<RelCollation> getCollations() {
            return ImmutableList.of();
        }


        @Override
        public RelDistribution getDistribution() {
            return null;
        }
    };

    private static final SchemaPlus ROOT_SCHEMA = PolyphenyDbSchema.createRootSchema( false ).add( "myDb", SCHEMA ).plus();


    private RelToSqlConverterTest.Sql sql( String sql ) {
        return new RelToSqlConverterTest.Sql( ROOT_SCHEMA, sql, PolyphenyDbSqlDialect.DEFAULT, RelToSqlConverterTest.DEFAULT_REL_CONFIG, ImmutableList.of() );
    }


    @Test
    public void testNestedSchemaSelectStar() {
        String query = "SELECT * FROM \"myTable\"";
        String expected = "SELECT \"a\", \"n1\".\"n11\".\"b\" AS \"n1\", \"n1\".\"n12\".\"c\" AS \"n12\", \"n2\".\"d\" AS \"n2\", \"e\"\n"
                + "FROM \"myDb\".\"myTable\"";
        sql( query ).ok( expected );
    }


    @Test
    public void testNestedSchemaRootColumns() {
        String query = "SELECT \"a\", \"e\" FROM \"myTable\"";
        String expected = "SELECT \"a\", \"e\"\n"
                + "FROM \"myDb\".\"myTable\"";
        sql( query ).ok( expected );
    }


    @Test
    public void testNestedSchemaNestedColumns() {
        String query = "SELECT \"a\", \"e\", "
                + "\"myTable\".\"n1\".\"n11\".\"b\", "
                + "\"myTable\".\"n2\".\"d\" "
                + "FROM \"myTable\"";
        String expected = "SELECT \"a\", "
                + "\"e\", "
                + "\"n1\".\"n11\".\"b\", "
                + "\"n2\".\"d\"\n"
                + "FROM \"myDb\".\"myTable\"";
        sql( query ).ok( expected );
    }
}

