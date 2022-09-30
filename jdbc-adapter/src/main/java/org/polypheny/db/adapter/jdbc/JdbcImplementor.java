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

package org.polypheny.db.adapter.jdbc;


import com.google.common.collect.ImmutableList;
import java.util.List;
import org.polypheny.db.adapter.java.JavaTypeFactory;
import org.polypheny.db.adapter.jdbc.rel2sql.AlgToSqlConverter;
import org.polypheny.db.algebra.AlgNode;
import org.polypheny.db.languages.ParserPos;
import org.polypheny.db.sql.language.SqlDialect;
import org.polypheny.db.sql.language.SqlIdentifier;
import org.polypheny.db.util.Util;


/**
 * State for generating a SQL statement.
 */
public class JdbcImplementor extends AlgToSqlConverter {

    private final JdbcSchema schema;


    public JdbcImplementor( SqlDialect dialect, JavaTypeFactory typeFactory, JdbcSchema schema ) {
        super( dialect );
        Util.discard( typeFactory );
        this.schema = schema;
    }


    /**
     * @see #dispatch
     */
    public Result visit( JdbcScan scan ) {
        return result( scan.jdbcTable.physicalTableName(), ImmutableList.of( Clause.FROM ), scan, null );
    }


    public Result implement( AlgNode node ) {
        return dispatch( node );
    }


    @Override
    public SqlIdentifier getPhysicalTableName( List<String> tableNames ) {
        JdbcTable table;
        if ( tableNames.size() == 1 ) {
            // only table name
            // NOTICE MV: I think, this case should no longer happen because there should always be a schema in the form
            //  <adapterUniqueName>_<logicalSchema>_<physicalSchema> be set.
            // TODO MV: Consider removing this case
            table = schema.getTableMap().get( tableNames.get( 0 ) );
        } else if ( tableNames.size() == 2 ) {
            // schema name and table name
            table = schema.getTableMap().get( tableNames.get( 1 ) );
        } else {
            throw new RuntimeException( "Unexpected number of names: " + tableNames.size() );
        }
        if ( table == null ) {
            throw new RuntimeException( "Unknown table: [ " + String.join( ", ", tableNames ) + " ] | Table Map : [ " + String.join( ", ", schema.getTableMap().keySet() ) );
        }
        return table.physicalTableName();
    }


    @Override
    public SqlIdentifier getPhysicalColumnName( List<String> tableNames, String columnName ) {
        if ( tableNames.size() == 1 ) {
            // only column name
            return schema.getTableMap().get( tableNames.get( 0 ) ).physicalColumnName( columnName );
        } else if ( tableNames.size() == 2 ) {
            // table name and column name
            JdbcTable table = schema.getTableMap().get( tableNames.get( 1 ) );
            if ( table.hasPhysicalColumnName( columnName ) ) {
                return schema.getTableMap().get( tableNames.get( 1 ) ).physicalColumnName( columnName );
            } else {
                return new SqlIdentifier( "_" + columnName, ParserPos.ZERO );
            }
        } else {
            throw new RuntimeException( "Unexpected number of names: " + tableNames.size() );
        }
    }

}

