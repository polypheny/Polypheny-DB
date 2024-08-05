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

import lombok.Getter;
import lombok.NonNull;
import lombok.extern.slf4j.Slf4j;
import org.apache.calcite.linq4j.tree.Expression;
import org.polypheny.db.adapter.Adapter;
import org.polypheny.db.adapter.DataContext;
import org.polypheny.db.adapter.jdbc.connection.ConnectionFactory;
import org.polypheny.db.adapter.jdbc.connection.ConnectionHandler;
import org.polypheny.db.adapter.jdbc.connection.ConnectionHandlerException;
import org.polypheny.db.catalog.entity.physical.PhysicalTable;
import org.polypheny.db.catalog.exceptions.GenericRuntimeException;
import org.polypheny.db.schema.Namespace;
import org.polypheny.db.schema.types.Expressible;
import org.polypheny.db.sql.language.SqlDialect;


/**
 * Implementation of {@link Namespace} that is backed by a JDBC data source.
 *
 * The tables in the JDBC data source appear to be tables in this schema; queries against this schema are executed
 * against those tables, pushing down as much as possible of the query logic to SQL.
 */
@Slf4j
public class JdbcSchema extends Namespace implements Expressible {

    final ConnectionFactory connectionFactory;
    public final SqlDialect dialect;

    @Getter
    private final JdbcConvention convention;

    public final Adapter<?> adapter;
    @Getter
    private final long id;


    /**
     * Creates a JDBC schema.
     *
     * @param connectionFactory Connection Factory
     * @param dialect SQL dialect
     * @param convention Calling convention
     */
    public JdbcSchema(
            long id,
            @NonNull ConnectionFactory connectionFactory,
            @NonNull SqlDialect dialect,
            JdbcConvention convention,
            Adapter<?> adapter ) {
        super( id, adapter.getAdapterId() );
        this.id = id;
        this.connectionFactory = connectionFactory;
        this.dialect = dialect;
        convention.setJdbcSchema( this );
        this.convention = convention;
        this.adapter = adapter;
    }


    public JdbcTable createJdbcTable(
            PhysicalTable table ) {
        return new JdbcTable(
                this,
                table
        );
    }


    public static JdbcSchema create(
            long id,
            String name,
            ConnectionFactory connectionFactory,
            SqlDialect dialect,
            Adapter<?> adapter ) {
        final Expression expression = adapter.getNamespaceAsExpression( id );
        final JdbcConvention convention = JdbcConvention.of( dialect, expression, name + adapter.adapterId ); // fixes multiple placement errors
        return new JdbcSchema( id, connectionFactory, dialect, convention, adapter );
    }


    // Used by generated code (see class JdbcToEnumerableConverter).
    public ConnectionHandler getConnectionHandler( DataContext dataContext ) {
        try {
            dataContext.getStatement().getTransaction().registerInvolvedAdapter( adapter );
            return connectionFactory.getOrCreateConnectionHandler( dataContext.getStatement().getTransaction().getXid() );
        } catch ( ConnectionHandlerException e ) {
            throw new GenericRuntimeException( e );
        }
    }


    @Override
    public Expression asExpression() {
        return this.adapter.getNamespaceAsExpression( id );
    }

}

