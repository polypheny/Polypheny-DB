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

package org.polypheny.db.adapter.jdbc;


import org.polypheny.db.adapter.java.JavaTypeFactory;
import org.polypheny.db.adapter.jdbc.rel2sql.RelToSqlConverter;
import org.polypheny.db.rel.RelNode;
import org.polypheny.db.sql.SqlDialect;
import org.polypheny.db.sql.SqlIdentifier;
import org.polypheny.db.util.Util;
import com.google.common.collect.ImmutableList;
import java.util.List;


/**
 * State for generating a SQL statement.
 */
public class JdbcImplementor extends RelToSqlConverter {

    private final JdbcSchema schema;


    public JdbcImplementor( SqlDialect dialect, JavaTypeFactory typeFactory, JdbcSchema schema ) {
        super( dialect );
        Util.discard( typeFactory );
        this.schema = schema;
    }


    /**
     * @see #dispatch
     */
    public Result visit( JdbcTableScan scan ) {
        return result( scan.jdbcTable.physicalTableName(), ImmutableList.of( Clause.FROM ), scan, null );
    }


    public Result implement( RelNode node ) {
        return dispatch( node );
    }


    @Override
    public SqlIdentifier getPhysicalTableName( List<String> tableNames ) {
        if ( tableNames.size() == 1 ) {
            // only table name
            return schema.getTableMap().get( tableNames.get( 0 ) ).physicalTableName();
        } else if ( tableNames.size() == 2 ) {
            // schema name and table name
            return schema.getTableMap().get( tableNames.get( 1 ) ).physicalTableName();
        } else {
            throw new RuntimeException( "Unexpected number of names: " + tableNames.size() );
        }
    }


    @Override
    public SqlIdentifier getPhysicalColumnName( List<String> tableNames, String columnName ) {
        if ( tableNames.size() == 1 ) {
            // only table name
            return schema.getTableMap().get( tableNames.get( 0 ) ).physicalColumnName( columnName );
        } else if ( tableNames.size() == 2 ) {
            // schema name and table name
            return schema.getTableMap().get( tableNames.get( 1 ) ).physicalColumnName( columnName );
        } else {
            throw new RuntimeException( "Unexpected number of names: " + tableNames.size() );
        }
    }

}

