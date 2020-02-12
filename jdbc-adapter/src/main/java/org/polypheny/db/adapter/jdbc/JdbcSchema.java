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


import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableMultimap;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Multimap;
import java.util.Collection;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import javax.sql.DataSource;
import lombok.Getter;
import lombok.NonNull;
import org.apache.calcite.linq4j.tree.Expression;
import org.polypheny.db.DataContext;
import org.polypheny.db.adapter.jdbc.connection.ConnectionFactory;
import org.polypheny.db.adapter.jdbc.connection.ConnectionHandler;
import org.polypheny.db.adapter.jdbc.connection.ConnectionHandlerException;
import org.polypheny.db.adapter.jdbc.stores.AbstractJdbcStore;
import org.polypheny.db.catalog.entity.CatalogColumn;
import org.polypheny.db.catalog.entity.CatalogColumnPlacement;
import org.polypheny.db.catalog.entity.combined.CatalogCombinedTable;
import org.polypheny.db.rel.type.RelDataType;
import org.polypheny.db.rel.type.RelDataTypeFactory;
import org.polypheny.db.rel.type.RelDataTypeImpl;
import org.polypheny.db.rel.type.RelDataTypeSystem;
import org.polypheny.db.rel.type.RelProtoDataType;
import org.polypheny.db.schema.Function;
import org.polypheny.db.schema.Schema;
import org.polypheny.db.schema.SchemaPlus;
import org.polypheny.db.schema.SchemaVersion;
import org.polypheny.db.schema.Schemas;
import org.polypheny.db.schema.Table;
import org.polypheny.db.sql.SqlDialect;
import org.polypheny.db.sql.SqlDialectFactory;
import org.polypheny.db.sql.type.SqlTypeFactoryImpl;
import org.polypheny.db.sql.type.SqlTypeName;
import org.polypheny.db.util.Util;


/**
 * Implementation of {@link Schema} that is backed by a JDBC data source.
 *
 * The tables in the JDBC data source appear to be tables in this schema; queries against this schema are executed
 * against those tables, pushing down as much as possible of the query logic to SQL.
 */
public class JdbcSchema implements Schema {

    final ConnectionFactory connectionFactory;
    final String schema;
    public final SqlDialect dialect;

    @Getter
    private final JdbcConvention convention;

    private final Map<String, JdbcTable> tableMap;
    private final Map<String, String> physicalToLogicalTableNameMap;

    private final AbstractJdbcStore jdbcStore;


    private JdbcSchema(
            @NonNull ConnectionFactory connectionFactory,
            @NonNull SqlDialect dialect,
            JdbcConvention convention,
            String schema,
            Map<String, JdbcTable> tableMap,
            Map<String, String> physicalToLogicalTableNameMap,
            AbstractJdbcStore jdbcStore ) {
        super();
        this.connectionFactory = connectionFactory;
        this.dialect = dialect;
        this.convention = convention;
        this.schema = schema;
        this.tableMap = tableMap;
        this.physicalToLogicalTableNameMap = physicalToLogicalTableNameMap;
        this.jdbcStore = jdbcStore;
    }


    /**
     * Creates a JDBC schema.
     *
     * @param connectionFactory Connection Factory
     * @param dialect SQL dialect
     * @param convention Calling convention
     * @param schema Schema name pattern
     */
    public JdbcSchema(
            @NonNull ConnectionFactory connectionFactory,
            @NonNull SqlDialect dialect,
            JdbcConvention convention,
            String schema,
            AbstractJdbcStore jdbcStore ) {
        super();
        this.connectionFactory = connectionFactory;
        this.dialect = dialect;
        convention.setJdbcSchema( this );
        this.convention = convention;
        this.schema = schema;
        this.tableMap = new HashMap<>();
        this.physicalToLogicalTableNameMap = new HashMap<>();
        this.jdbcStore = jdbcStore;
    }


    public JdbcTable createJdbcTable( CatalogCombinedTable combinedTable ) {
        // Temporary type factory, just for the duration of this method. Allowable because we're creating a proto-type,
        // not a type; before being used, the proto-type will be copied into a real type factory.
        final RelDataTypeFactory typeFactory = new SqlTypeFactoryImpl( RelDataTypeSystem.DEFAULT );
        final RelDataTypeFactory.Builder fieldInfo = typeFactory.builder();
        List<String> logicalColumnNames = new LinkedList<>();
        List<String> physicalColumnNames = new LinkedList<>();
        String physicalSchemaName = null;
        String physicalTableName = null;
        for ( CatalogColumnPlacement placement : combinedTable.getColumnPlacementsByStore().get( jdbcStore.getStoreId() ) ) {
            CatalogColumn catalogColumn = null;
            // TODO MV: This is not really efficient
            // Get catalog column
            for ( CatalogColumn c : combinedTable.getColumns() ) {
                if ( c.id == placement.columnId ) {
                    catalogColumn = c;
                }
            }
            if ( catalogColumn == null ) {
                throw new RuntimeException( "Column not found." ); // This should not happen
            }
            if ( physicalSchemaName == null ) {
                physicalSchemaName = placement.physicalSchemaName;
            }
            if ( physicalTableName == null ) {
                physicalTableName = placement.physicalTableName;
            }
            SqlTypeName dataTypeName = SqlTypeName.get( catalogColumn.type.name() ); // TODO MV: Replace PolySqlType with native
            RelDataType sqlType = sqlType( typeFactory, dataTypeName, catalogColumn.length, catalogColumn.scale, null );
            fieldInfo.add( catalogColumn.name, placement.physicalColumnName, sqlType ).nullable( catalogColumn.nullable );
            logicalColumnNames.add( catalogColumn.name );
            physicalColumnNames.add( placement.physicalColumnName );
        }
        JdbcTable table = new JdbcTable(
                this,
                combinedTable.getSchema().name,
                combinedTable.getTable().name,
                logicalColumnNames,
                TableType.valueOf( combinedTable.getTable().tableType.name() ),
                RelDataTypeImpl.proto( fieldInfo.build() ),
                physicalSchemaName,
                physicalTableName,
                physicalColumnNames );
        tableMap.put( combinedTable.getTable().name, table );
        physicalToLogicalTableNameMap.put( physicalTableName, combinedTable.getTable().name );
        return table;
    }


    public static JdbcSchema create(
            SchemaPlus parentSchema,
            String name,
            ConnectionFactory connectionFactory,
            SqlDialect dialect,
            String schema,
            AbstractJdbcStore jdbcStore ) {
        final Expression expression = Schemas.subSchemaExpression( parentSchema, name, JdbcSchema.class );
        final JdbcConvention convention = JdbcConvention.of( dialect, expression, name );
        return new JdbcSchema( connectionFactory, dialect, convention, schema, jdbcStore );
    }


    /**
     * Returns a suitable SQL dialect for the given data source.
     */
    public static SqlDialect createDialect( SqlDialectFactory dialectFactory, DataSource dataSource ) {
        return JdbcUtils.DialectPool.INSTANCE.get( dialectFactory, dataSource );
    }


    @Override
    public boolean isMutable() {
        return true;
    }


    @Override
    public Schema snapshot( SchemaVersion version ) {
        return new JdbcSchema(
                connectionFactory,
                dialect,
                convention,
                schema,
                tableMap,
                physicalToLogicalTableNameMap,
                jdbcStore );
    }


    // Used by generated code (see class JdbcToEnumerableConverter).
    public ConnectionHandler getConnectionHandler( DataContext dataContext ) {
        try {
            dataContext.getTransaction().registerInvolvedStore( jdbcStore );
            return connectionFactory.getOrCreateConnectionHandler( dataContext.getTransaction().getXid() );
        } catch ( ConnectionHandlerException e ) {
            throw new RuntimeException( e );
        }
    }


    @Override
    public Expression getExpression( SchemaPlus parentSchema, String name ) {
        return Schemas.subSchemaExpression( parentSchema, name, JdbcSchema.class );
    }


    protected Multimap<String, Function> getFunctions() {
        // TODO: populate map from JDBC metadata
        return ImmutableMultimap.of();
    }


    @Override
    public final Collection<Function> getFunctions( String name ) {
        return getFunctions().get( name ); // never null
    }


    @Override
    public final Set<String> getFunctionNames() {
        return getFunctions().keySet();
    }


    @Override
    public Table getTable( String name ) {
        return getTableMap().get( name );
    }


    public synchronized ImmutableMap<String, JdbcTable> getTableMap() {
        return ImmutableMap.copyOf( tableMap );
    }


    private RelDataType sqlType( RelDataTypeFactory typeFactory, SqlTypeName dataTypeName, Integer precision, Integer scale, String typeString ) {
        // Fall back to ANY if type is unknown
        final SqlTypeName sqlTypeName = Util.first( dataTypeName, SqlTypeName.ANY );
        switch ( sqlTypeName ) {
            case ARRAY:
                RelDataType component = null;
                if ( typeString != null && typeString.endsWith( " ARRAY" ) ) {
                    // E.g. hsqldb gives "INTEGER ARRAY", so we deduce the component type "INTEGER".
                    final String remaining = typeString.substring( 0, typeString.length() - " ARRAY".length() );
                    component = parseTypeString( typeFactory, remaining );
                }
                if ( component == null ) {
                    component = typeFactory.createTypeWithNullability( typeFactory.createSqlType( SqlTypeName.ANY ), true );
                }
                return typeFactory.createArrayType( component, -1 );
        }
        if ( precision != null && scale != null && sqlTypeName.allowsPrecScale( true, true ) ) {
            return typeFactory.createSqlType( sqlTypeName, precision, scale );
        } else if ( precision != null && sqlTypeName.allowsPrecNoScale() ) {
            return typeFactory.createSqlType( sqlTypeName, precision );
        } else {
            assert sqlTypeName.allowsNoPrecNoScale();
            return typeFactory.createSqlType( sqlTypeName );
        }
    }


    /**
     * Given "INTEGER", returns BasicSqlType(INTEGER).
     * Given "VARCHAR(10)", returns BasicSqlType(VARCHAR, 10).
     * Given "NUMERIC(10, 2)", returns BasicSqlType(NUMERIC, 10, 2).
     */
    private RelDataType parseTypeString( RelDataTypeFactory typeFactory, String typeString ) {
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
            final SqlTypeName typeName = SqlTypeName.valueOf( typeString );
            return typeName.allowsPrecScale( true, true )
                    ? typeFactory.createSqlType( typeName, precision, scale )
                    : typeName.allowsPrecScale( true, false )
                            ? typeFactory.createSqlType( typeName, precision )
                            : typeFactory.createSqlType( typeName );
        } catch ( IllegalArgumentException e ) {
            return typeFactory.createTypeWithNullability( typeFactory.createSqlType( SqlTypeName.ANY ), true );
        }
    }


    @Override
    public Set<String> getTableNames() {
        // This method is called during a cache refresh. We can take it as a signal that we need to re-build our own cache.
        return getTableMap().keySet();
    }


    protected Map<String, RelProtoDataType> getTypes() {
        // TODO: populate map from JDBC metadata
        return ImmutableMap.of();
    }


    @Override
    public RelProtoDataType getType( String name ) {
        return getTypes().get( name );
    }


    @Override
    public Set<String> getTypeNames() {
        return getTypes().keySet();
    }


    @Override
    public Schema getSubSchema( String name ) {
        // JDBC does not support sub-schemas.
        return null;
    }


    @Override
    public Set<String> getSubSchemaNames() {
        return ImmutableSet.of();
    }

}

