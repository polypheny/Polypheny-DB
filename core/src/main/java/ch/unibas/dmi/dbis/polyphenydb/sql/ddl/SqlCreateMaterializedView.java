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
 * The MIT License (MIT)
 *
 * Copyright (c) 2019 Databases and Information Systems Research Group, University of Basel, Switzerland
 *
 * Permission is hereby granted, free of charge, to any person obtaining a
 * copy of this software and associated documentation files (the "Software"), to deal
 * in the Software without restriction, including without limitation the rights
 * to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
 * copies of the Software, and to permit persons to whom the Software is
 * furnished to do so, subject to the following conditions:
 *
 * The above copyright notice and this permission notice shall be included in all
 * copies or substantial portions of the Software.
 *
 * THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
 * IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
 * FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
 * AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
 * LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
 * OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
 * SOFTWARE.
 */

package ch.unibas.dmi.dbis.polyphenydb.sql.ddl;


import ch.unibas.dmi.dbis.polyphenydb.Transaction;
import ch.unibas.dmi.dbis.polyphenydb.jdbc.Context;
import ch.unibas.dmi.dbis.polyphenydb.materialize.MaterializationKey;
import ch.unibas.dmi.dbis.polyphenydb.rel.type.RelProtoDataType;
import ch.unibas.dmi.dbis.polyphenydb.schema.Schema;
import ch.unibas.dmi.dbis.polyphenydb.sql.SqlCreate;
import ch.unibas.dmi.dbis.polyphenydb.sql.SqlExecutableStatement;
import ch.unibas.dmi.dbis.polyphenydb.sql.SqlIdentifier;
import ch.unibas.dmi.dbis.polyphenydb.sql.SqlKind;
import ch.unibas.dmi.dbis.polyphenydb.sql.SqlNode;
import ch.unibas.dmi.dbis.polyphenydb.sql.SqlNodeList;
import ch.unibas.dmi.dbis.polyphenydb.sql.SqlOperator;
import ch.unibas.dmi.dbis.polyphenydb.sql.SqlSpecialOperator;
import ch.unibas.dmi.dbis.polyphenydb.sql.SqlWriter;
import ch.unibas.dmi.dbis.polyphenydb.sql.parser.SqlParserPos;
import ch.unibas.dmi.dbis.polyphenydb.sql2rel.NullInitializerExpressionFactory;
import ch.unibas.dmi.dbis.polyphenydb.util.ImmutableNullableList;
import java.util.List;
import java.util.Objects;


/**
 * Parse tree for {@code CREATE MATERIALIZED VIEW} statement.
 */
public class SqlCreateMaterializedView extends SqlCreate implements SqlExecutableStatement {

    private final SqlIdentifier name;
    private final SqlNodeList columnList;
    private final SqlNode query;

    private static final SqlOperator OPERATOR = new SqlSpecialOperator( "CREATE MATERIALIZED VIEW", SqlKind.CREATE_MATERIALIZED_VIEW );


    /**
     * Creates a SqlCreateView.
     */
    SqlCreateMaterializedView( SqlParserPos pos, boolean replace, boolean ifNotExists, SqlIdentifier name, SqlNodeList columnList, SqlNode query ) {
        super( OPERATOR, pos, replace, ifNotExists );
        this.name = Objects.requireNonNull( name );
        this.columnList = columnList; // may be null
        this.query = Objects.requireNonNull( query );
    }


    public List<SqlNode> getOperandList() {
        return ImmutableNullableList.of( name, columnList, query );
    }


    @Override
    public void execute( Context context, Transaction transaction ) {
        throw new RuntimeException( "Not supported yet" );
    }


    @Override
    public void unparse( SqlWriter writer, int leftPrec, int rightPrec ) {
        writer.keyword( "CREATE" );
        writer.keyword( "MATERIALIZED VIEW" );
        if ( ifNotExists ) {
            writer.keyword( "IF NOT EXISTS" );
        }
        name.unparse( writer, leftPrec, rightPrec );
        if ( columnList != null ) {
            SqlWriter.Frame frame = writer.startList( "(", ")" );
            for ( SqlNode c : columnList ) {
                writer.sep( "," );
                c.unparse( writer, 0, 0 );
            }
            writer.endList( frame );
        }
        writer.keyword( "AS" );
        writer.newlineAndIndent();
        query.unparse( writer, 0, 0 );
    }


    /**
     * A table that implements a materialized view.
     */
    private static class MaterializedViewTable extends SqlCreateTable.MutableArrayTable {

        /**
         * The key with which this was stored in the materialization service, or null if not (yet) materialized.
         */
        MaterializationKey key;


        MaterializedViewTable( String name, RelProtoDataType protoRowType ) {
            super( name, protoRowType, protoRowType, NullInitializerExpressionFactory.INSTANCE );
        }


        @Override
        public Schema.TableType getJdbcTableType() {
            return Schema.TableType.MATERIALIZED_VIEW;
        }


        @Override
        public <C> C unwrap( Class<C> aClass ) {
            if ( MaterializationKey.class.isAssignableFrom( aClass ) && aClass.isInstance( key ) ) {
                return aClass.cast( key );
            }
            return super.unwrap( aClass );
        }
    }


    /*
    public void execute( Context context ) {
        final Pair<PolyphenyDbSchema, String> pair = SqlDdlNodes.schema( context, true, name );
        if ( pair.left.plus().getTable( pair.right ) != null ) {
            // Materialized view exists.
            if ( !ifNotExists ) {
                // They did not specify IF NOT EXISTS, so give error.
                throw SqlUtil.newContextException( name.getParserPosition(), RESOURCE.tableExists( pair.right ) );
            }
            return;
        }
        final SqlNode q = SqlDdlNodes.renameColumns( columnList, query );
        final String sql = q.toSqlString( PolyphenyDbSqlDialect.DEFAULT ).getSql();
        final List<String> schemaPath = pair.left.path( null );
        final ViewTableMacro viewTableMacro =
                ViewTable.viewMacro(
                        pair.left.plus(),
                        sql,
                        schemaPath,
                        context.getObjectPath(),
                        false );
        final TranslatableTable x = viewTableMacro.apply( ImmutableList.of() );
        final RelDataType rowType = x.getRowType( context.getTypeFactory() );

        // Table does not exist. Create it.
        final MaterializedViewTable table = new MaterializedViewTable( pair.right, RelDataTypeImpl.proto( rowType ) );
        pair.left.add( pair.right, table );
        SqlDdlNodes.populate( name, query, context );
        table.key = MaterializationService.instance().defineMaterialization( pair.left, null, sql, schemaPath, pair.right, true, true );
    } */
}

