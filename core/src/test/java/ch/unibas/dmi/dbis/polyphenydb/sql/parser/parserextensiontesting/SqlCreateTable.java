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

package ch.unibas.dmi.dbis.polyphenydb.sql.parser.parserextensiontesting;


import ch.unibas.dmi.dbis.polyphenydb.adapter.java.JavaTypeFactory;
import ch.unibas.dmi.dbis.polyphenydb.jdbc.Context;
import ch.unibas.dmi.dbis.polyphenydb.jdbc.JavaTypeFactoryImpl;
import ch.unibas.dmi.dbis.polyphenydb.jdbc.PolyphenyDbSchema;
import ch.unibas.dmi.dbis.polyphenydb.rel.type.RelDataType;
import ch.unibas.dmi.dbis.polyphenydb.rel.type.RelDataTypeFactory;
import ch.unibas.dmi.dbis.polyphenydb.rel.type.RelDataTypeImpl;
import ch.unibas.dmi.dbis.polyphenydb.rel.type.RelProtoDataType;
import ch.unibas.dmi.dbis.polyphenydb.schema.SchemaPlus;
import ch.unibas.dmi.dbis.polyphenydb.schema.Schemas;
import ch.unibas.dmi.dbis.polyphenydb.schema.impl.AbstractTableQueryable;
import ch.unibas.dmi.dbis.polyphenydb.sql.SqlCreate;
import ch.unibas.dmi.dbis.polyphenydb.sql.SqlDataTypeSpec;
import ch.unibas.dmi.dbis.polyphenydb.sql.SqlExecutableStatement;
import ch.unibas.dmi.dbis.polyphenydb.sql.SqlIdentifier;
import ch.unibas.dmi.dbis.polyphenydb.sql.SqlKind;
import ch.unibas.dmi.dbis.polyphenydb.sql.SqlNode;
import ch.unibas.dmi.dbis.polyphenydb.sql.SqlNodeList;
import ch.unibas.dmi.dbis.polyphenydb.sql.SqlOperator;
import ch.unibas.dmi.dbis.polyphenydb.sql.SqlSpecialOperator;
import ch.unibas.dmi.dbis.polyphenydb.sql.SqlWriter;
import ch.unibas.dmi.dbis.polyphenydb.sql.parser.SqlParserPos;
import ch.unibas.dmi.dbis.polyphenydb.test.JdbcTest;
import ch.unibas.dmi.dbis.polyphenydb.util.Pair;
import ch.unibas.dmi.dbis.polyphenydb.util.Util;
import com.google.common.collect.ImmutableList;
import java.lang.reflect.Type;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import org.apache.calcite.linq4j.Enumerator;
import org.apache.calcite.linq4j.Linq4j;
import org.apache.calcite.linq4j.QueryProvider;
import org.apache.calcite.linq4j.Queryable;
import org.apache.calcite.linq4j.tree.Expression;


/**
 * Simple test example of a CREATE TABLE statement.
 */
public class SqlCreateTable extends SqlCreate implements SqlExecutableStatement {

    private final SqlIdentifier name;
    private final SqlNodeList columnList;

    private static final SqlOperator OPERATOR = new SqlSpecialOperator( "CREATE TABLE", SqlKind.CREATE_TABLE );


    /**
     * Creates a SqlCreateTable.
     */
    public SqlCreateTable( SqlParserPos pos, SqlIdentifier name, SqlNodeList columnList ) {
        super( OPERATOR, pos, false, false );
        this.name = name;
        this.columnList = columnList;
    }


    @Override
    public List<SqlNode> getOperandList() {
        return ImmutableList.of( name, columnList );
    }


    @Override
    public void unparse( SqlWriter writer, int leftPrec, int rightPrec ) {
        writer.keyword( "CREATE" );
        writer.keyword( "TABLE" );
        name.unparse( writer, leftPrec, rightPrec );
        SqlWriter.Frame frame = writer.startList( "(", ")" );
        for ( Pair<SqlIdentifier, SqlDataTypeSpec> pair : nameTypes() ) {
            writer.sep( "," );
            pair.left.unparse( writer, leftPrec, rightPrec ); // name
            pair.right.unparse( writer, leftPrec, rightPrec ); // type
            if ( Boolean.FALSE.equals( pair.right.getNullable() ) ) {
                writer.keyword( "NOT NULL" );
            }
        }
        writer.endList( frame );
    }


    /**
     * Creates a list of (name, type) pairs from {@link #columnList}, in which they alternate.
     */
    private List<Pair<SqlIdentifier, SqlDataTypeSpec>> nameTypes() {
        final List list = columnList.getList();
        //noinspection unchecked
        return Pair.zip( (List<SqlIdentifier>) Util.quotientList( list, 2, 0 ), Util.quotientList( (List<SqlDataTypeSpec>) list, 2, 1 ) );
    }


    public void execute( Context context ) {
        final List<String> path = context.getDefaultSchemaPath();
        PolyphenyDbSchema schema = context.getRootSchema();
        for ( String p : path ) {
            schema = schema.getSubSchema( p, true );
        }
        final JavaTypeFactory typeFactory = new JavaTypeFactoryImpl();
        final RelDataTypeFactory.Builder builder = typeFactory.builder();
        for ( Pair<SqlIdentifier, SqlDataTypeSpec> pair : nameTypes() ) {
            builder.add( pair.left.getSimple(), pair.right.deriveType( typeFactory, true ) );
        }
        final RelDataType rowType = builder.build();
        schema.add( name.getSimple(), new MutableArrayTable( name.getSimple(), RelDataTypeImpl.proto( rowType ) ) );
    }


    /**
     * Table backed by a Java list.
     */
    private static class MutableArrayTable extends JdbcTest.AbstractModifiableTable {

        final List list = new ArrayList();
        private final RelProtoDataType protoRowType;


        MutableArrayTable( String name, RelProtoDataType protoRowType ) {
            super( name );
            this.protoRowType = protoRowType;
        }


        public Collection getModifiableCollection() {
            return list;
        }


        public <T> Queryable<T> asQueryable( QueryProvider queryProvider, SchemaPlus schema, String tableName ) {
            return new AbstractTableQueryable<T>( queryProvider, schema, this, tableName ) {
                public Enumerator<T> enumerator() {
                    //noinspection unchecked
                    return (Enumerator<T>) Linq4j.enumerator( list );
                }
            };
        }


        public Type getElementType() {
            return Object[].class;
        }


        public Expression getExpression( SchemaPlus schema, String tableName, Class clazz ) {
            return Schemas.tableExpression( schema, getElementType(), tableName, clazz );
        }


        public RelDataType getRowType( RelDataTypeFactory typeFactory ) {
            return protoRowType.apply( typeFactory );
        }
    }
}

