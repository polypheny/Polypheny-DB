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

package org.polypheny.db.adapter.jdbc.rel2sql;


import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import java.util.Collection;
import java.util.List;
import java.util.Set;
import org.apache.calcite.linq4j.tree.Expression;
import org.junit.Test;
import org.polypheny.db.algebra.AlgCollation;
import org.polypheny.db.algebra.AlgDistribution;
import org.polypheny.db.algebra.AlgReferentialConstraint;
import org.polypheny.db.algebra.type.AlgDataType;
import org.polypheny.db.algebra.type.AlgDataTypeFactory;
import org.polypheny.db.algebra.type.AlgProtoDataType;
import org.polypheny.db.catalog.Catalog.NamespaceType;
import org.polypheny.db.nodes.Call;
import org.polypheny.db.nodes.Node;
import org.polypheny.db.schema.AbstractPolyphenyDbSchema;
import org.polypheny.db.schema.Function;
import org.polypheny.db.schema.Schema;
import org.polypheny.db.schema.SchemaPlus;
import org.polypheny.db.schema.SchemaVersion;
import org.polypheny.db.schema.Statistic;
import org.polypheny.db.schema.Table;
import org.polypheny.db.sql.SqlLanguagelDependant;
import org.polypheny.db.sql.language.dialect.PolyphenyDbSqlDialect;
import org.polypheny.db.type.PolyType;
import org.polypheny.db.util.ImmutableBitSet;


/**
 * Tests for {@link AlgToSqlConverter} on a schema that has nested structures of multiple levels.
 */
public class RelToSqlConverterStructsTest extends SqlLanguagelDependant {

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
        public AlgProtoDataType getType( String name ) {
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
        public AlgDataType getRowType( AlgDataTypeFactory typeFactory ) {
            final AlgDataType aType = typeFactory.createPolyType( PolyType.BIGINT );
            final AlgDataType bType = typeFactory.createPolyType( PolyType.BIGINT );
            final AlgDataType cType = typeFactory.createPolyType( PolyType.BIGINT );
            final AlgDataType dType = typeFactory.createPolyType( PolyType.BIGINT );
            final AlgDataType eType = typeFactory.createPolyType( PolyType.BIGINT );
            final AlgDataType n11Type = typeFactory.createStructType( ImmutableList.of( bType ), ImmutableList.of( "b" ) );
            final AlgDataType n12Type = typeFactory.createStructType( ImmutableList.of( cType ), ImmutableList.of( "c" ) );
            final AlgDataType n1Type = typeFactory.createStructType( ImmutableList.of( n11Type, n12Type ), ImmutableList.of( "n11", "n12" ) );
            final AlgDataType n2Type = typeFactory.createStructType( ImmutableList.of( dType ), ImmutableList.of( "d" ) );
            return typeFactory.createStructType(
                    ImmutableList.of( aType, n1Type, n2Type, eType ),
                    ImmutableList.of( "a", "n1", "n2", "e" ) );
        }


        @Override
        public Statistic getStatistic() {
            return STATS;
        }


        @Override
        public Long getTableId() {
            return null;
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
        public boolean rolledUpColumnValidInsideAgg( String column, Call call, Node parent ) {
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
        public List<AlgReferentialConstraint> getReferentialConstraints() {
            return ImmutableList.of();
        }


        @Override
        public List<AlgCollation> getCollations() {
            return ImmutableList.of();
        }


        @Override
        public AlgDistribution getDistribution() {
            return null;
        }
    };

    private static final SchemaPlus ROOT_SCHEMA = AbstractPolyphenyDbSchema.createRootSchema( "" ).add( "myDb", SCHEMA, NamespaceType.RELATIONAL ).plus();


    private AlgToSqlConverterTest.Sql sql( String sql ) {
        return new AlgToSqlConverterTest.Sql(
                ROOT_SCHEMA,
                sql,
                PolyphenyDbSqlDialect.DEFAULT,
                AlgToSqlConverterTest.DEFAULT_REL_CONFIG,
                ImmutableList.of() );
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

