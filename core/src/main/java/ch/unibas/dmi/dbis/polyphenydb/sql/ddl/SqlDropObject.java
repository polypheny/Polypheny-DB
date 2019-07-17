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


import static ch.unibas.dmi.dbis.polyphenydb.util.Static.RESOURCE;

import ch.unibas.dmi.dbis.polyphenydb.jdbc.Context;
import ch.unibas.dmi.dbis.polyphenydb.jdbc.PolyphenyDbSchema;
import ch.unibas.dmi.dbis.polyphenydb.sql.SqlDrop;
import ch.unibas.dmi.dbis.polyphenydb.sql.SqlExecutableStatement;
import ch.unibas.dmi.dbis.polyphenydb.sql.SqlIdentifier;
import ch.unibas.dmi.dbis.polyphenydb.sql.SqlNode;
import ch.unibas.dmi.dbis.polyphenydb.sql.SqlOperator;
import ch.unibas.dmi.dbis.polyphenydb.sql.SqlUtil;
import ch.unibas.dmi.dbis.polyphenydb.sql.SqlWriter;
import ch.unibas.dmi.dbis.polyphenydb.sql.parser.SqlParserPos;
import com.google.common.collect.ImmutableList;
import java.util.List;


/**
 * Base class for parse trees of {@code DROP TABLE}, {@code DROP VIEW}, {@code DROP MATERIALIZED VIEW} and {@code DROP TYPE} statements.
 */
abstract class SqlDropObject extends SqlDrop implements SqlExecutableStatement {

    protected final SqlIdentifier name;


    /**
     * Creates a SqlDropObject.
     */
    SqlDropObject( SqlOperator operator, SqlParserPos pos, boolean ifExists, SqlIdentifier name ) {
        super( operator, pos, ifExists );
        this.name = name;
    }


    public List<SqlNode> getOperandList() {
        return ImmutableList.of( name );
    }


    @Override
    public void unparse( SqlWriter writer, int leftPrec, int rightPrec ) {
        writer.keyword( getOperator().getName() ); // "DROP TABLE" etc.
        if ( ifExists ) {
            writer.keyword( "IF EXISTS" );
        }
        name.unparse( writer, leftPrec, rightPrec );
    }


    public void execute( Context context ) {
        final List<String> path = context.getDefaultSchemaPath();
        PolyphenyDbSchema schema = context.getRootSchema();
        for ( String p : path ) {
            schema = schema.getSubSchema( p, true );
        }
        final boolean existed;
        switch ( getKind() ) {
            case DROP_TABLE:
            case DROP_MATERIALIZED_VIEW:
                existed = schema.removeTable( name.getSimple() );
                if ( !existed && !ifExists ) {
                    throw SqlUtil.newContextException( name.getParserPosition(), RESOURCE.tableNotFound( name.getSimple() ) );
                }
                break;
            case DROP_VIEW:
                // Not quite right: removes any other functions with the same name
                existed = schema.removeFunction( name.getSimple() );
                if ( !existed && !ifExists ) {
                    throw SqlUtil.newContextException( name.getParserPosition(), RESOURCE.viewNotFound( name.getSimple() ) );
                }
                break;
            case DROP_TYPE:
                existed = schema.removeType( name.getSimple() );
                if ( !existed && !ifExists ) {
                    throw SqlUtil.newContextException( name.getParserPosition(), RESOURCE.typeNotFound( name.getSimple() ) );
                }
                break;
            case OTHER_DDL:
            default:
                throw new AssertionError( getKind() );
        }
    }
}
