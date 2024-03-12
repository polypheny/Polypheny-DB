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

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableMultimap;
import com.google.common.collect.Multimap;
import java.util.HashMap;
import java.util.Map;
import lombok.Getter;
import lombok.NonNull;
import lombok.extern.slf4j.Slf4j;
import org.apache.calcite.linq4j.tree.Expression;
import org.polypheny.db.adapter.Adapter;
import org.polypheny.db.adapter.DataContext;
import org.polypheny.db.adapter.jdbc.connection.ConnectionFactory;
import org.polypheny.db.adapter.jdbc.connection.ConnectionHandler;
import org.polypheny.db.adapter.jdbc.connection.ConnectionHandlerException;
import org.polypheny.db.algebra.type.AlgDataType;
import org.polypheny.db.algebra.type.AlgDataTypeFactory;
import org.polypheny.db.algebra.type.AlgProtoDataType;
import org.polypheny.db.catalog.entity.physical.PhysicalTable;
import org.polypheny.db.catalog.exceptions.GenericRuntimeException;
import org.polypheny.db.schema.Function;
import org.polypheny.db.schema.Namespace;
import org.polypheny.db.schema.types.Expressible;
import org.polypheny.db.sql.language.SqlDialect;
import org.polypheny.db.type.PolyType;


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

    private final Map<String, JdbcTable> tableMap;

    public final Adapter<?> adapter;
    @Getter
    private final long id;


    private JdbcSchema(
            long id,
            @NonNull ConnectionFactory connectionFactory,
            @NonNull SqlDialect dialect,
            JdbcConvention convention,
            Map<String, JdbcTable> tableMap,
            Adapter<?> adapter ) {
        super( id, adapter.getAdapterId() );
        this.id = id;
        this.connectionFactory = connectionFactory;
        this.dialect = dialect;
        this.convention = convention;
        this.tableMap = tableMap;
        this.adapter = adapter;

    }


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
        this.tableMap = new HashMap<>();
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


    protected Multimap<String, Function> getFunctions() {
        // TODO: populate map from JDBC metadata
        return ImmutableMultimap.of();
    }


    public synchronized ImmutableMap<String, JdbcTable> getTableMap() {
        return ImmutableMap.copyOf( tableMap );
    }


    /**
     * Given "INTEGER", returns BasicSqlType(INTEGER).
     * Given "VARCHAR(10)", returns BasicSqlType(VARCHAR, 10).
     * Given "NUMERIC(10, 2)", returns BasicSqlType(NUMERIC, 10, 2).
     */
    private AlgDataType parseTypeString( AlgDataTypeFactory typeFactory, String typeString ) {
        int precision = -1;
        int scale = -1;
        int open = typeString.indexOf( "(" );
        if ( open >= 0 ) {
            int close = typeString.indexOf( ")", open );
            if ( close >= 0 ) {
                String rest = typeString.substring( open + 1, close );
                typeString = typeString.substring( 0, open );
                int comma = rest.indexOf( "," );
                if ( comma >= 0 ) {
                    precision = Integer.parseInt( rest.substring( 0, comma ) );
                    scale = Integer.parseInt( rest.substring( comma ) );
                } else {
                    precision = Integer.parseInt( rest );
                }
            }
        }
        try {
            final PolyType typeName = PolyType.valueOf( typeString );
            return typeName.allowsPrecScale( true, true )
                    ? typeFactory.createPolyType( typeName, precision, scale )
                    : typeName.allowsPrecScale( true, false )
                            ? typeFactory.createPolyType( typeName, precision )
                            : typeFactory.createPolyType( typeName );
        } catch ( IllegalArgumentException e ) {
            return typeFactory.createTypeWithNullability( typeFactory.createPolyType( PolyType.ANY ), true );
        }
    }


    protected Map<String, AlgProtoDataType> getTypes() {
        // TODO: populate map from JDBC metadata
        return ImmutableMap.of();
    }


    @Override
    public Expression asExpression() {
        return this.adapter.getNamespaceAsExpression( id ); //todo change
    }

}

