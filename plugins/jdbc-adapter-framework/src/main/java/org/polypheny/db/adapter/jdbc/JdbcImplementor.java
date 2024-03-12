/*
 * Copyright 2019-2024 The Polypheny Project
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
import com.google.common.collect.ImmutableMap;
import java.util.Arrays;
import java.util.HashMap;
import java.util.function.Function;
import lombok.Getter;
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
@Getter
public class JdbcImplementor extends AlgToSqlConverter {

    private final ImmutableMap<Class<? extends AlgNode>, Function<AlgNode, Result>> handlers;


    public JdbcImplementor( SqlDialect dialect, JavaTypeFactory typeFactory, JdbcSchema schema ) {
        super( dialect );
        Util.discard( typeFactory );

        handlers = ImmutableMap.copyOf( new HashMap<>() {{
            putAll( JdbcImplementor.super.getHandlers() );
            put( JdbcScan.class, s -> accept( (JdbcScan) s ) );
        }} );
    }


    public Result accept( JdbcScan scan ) {
        return result( scan.jdbcTable.physicalTableName(), ImmutableList.of( Clause.FROM ), scan, null );
    }


    public Result implement( AlgNode node ) {
        return handle( node );
    }


    @Override
    public SqlIdentifier getPhysicalTableName( JdbcTable physical ) {
        return new SqlIdentifier( Arrays.asList( physical.namespaceName, physical.name ), ParserPos.ZERO );
    }


}

