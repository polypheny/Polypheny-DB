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

package ch.unibas.dmi.dbis.polyphenydb.adapter.jdbc;


import ch.unibas.dmi.dbis.polyphenydb.catalog.entity.CatalogColumn;
import ch.unibas.dmi.dbis.polyphenydb.catalog.entity.combined.CatalogCombinedTable;
import ch.unibas.dmi.dbis.polyphenydb.rel.type.RelDataType;
import ch.unibas.dmi.dbis.polyphenydb.rel.type.RelDataTypeFactory;
import ch.unibas.dmi.dbis.polyphenydb.rel.type.RelDataTypeImpl;
import ch.unibas.dmi.dbis.polyphenydb.rel.type.RelDataTypeSystem;
import ch.unibas.dmi.dbis.polyphenydb.rel.type.RelProtoDataType;
import ch.unibas.dmi.dbis.polyphenydb.schema.Function;
import ch.unibas.dmi.dbis.polyphenydb.schema.Schema;
import ch.unibas.dmi.dbis.polyphenydb.schema.SchemaPlus;
import ch.unibas.dmi.dbis.polyphenydb.schema.SchemaVersion;
import ch.unibas.dmi.dbis.polyphenydb.schema.Schemas;
import ch.unibas.dmi.dbis.polyphenydb.schema.Table;
import ch.unibas.dmi.dbis.polyphenydb.sql.SqlDialect;
import ch.unibas.dmi.dbis.polyphenydb.sql.SqlDialectFactory;
import ch.unibas.dmi.dbis.polyphenydb.sql.SqlDialectFactoryImpl;
import ch.unibas.dmi.dbis.polyphenydb.sql.type.SqlTypeFactoryImpl;
import ch.unibas.dmi.dbis.polyphenydb.sql.type.SqlTypeName;
import ch.unibas.dmi.dbis.polyphenydb.util.Util;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableMultimap;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Multimap;
import java.util.Collection;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import javax.sql.DataSource;
import lombok.Getter;
import org.apache.calcite.linq4j.tree.Expression;


/**
 * Implementation of {@link Schema} that is backed by a JDBC data source.
 *
 * The tables in the JDBC data source appear to be tables in this schema; queries against this schema are executed against those tables, pushing down as much as possible of the query logic to SQL.
 */
public class JdbcSchema implements Schema {

    final DataSource dataSource;
    final String database;
    final String schema;
    public final SqlDialect dialect;

    @Getter
    private final JdbcConvention convention;

    private final Map<String, JdbcTable> tableMap;


    private JdbcSchema( DataSource dataSource, SqlDialect dialect, JdbcConvention convention, String catalog, String schema, Map<String, JdbcTable> tableMap ) {
        super();
        this.dataSource = Objects.requireNonNull( dataSource );
        this.dialect = Objects.requireNonNull( dialect );
        this.convention = convention;
        this.database = catalog;
        this.schema = schema;
        this.tableMap = tableMap;
    }


    /**
     * Creates a JDBC schema.
     *
     * @param dataSource Data source
     * @param dialect SQL dialect
     * @param convention Calling convention
     * @param database Database name, or null
     * @param schema Schema name pattern
     */
    public JdbcSchema( DataSource dataSource, SqlDialect dialect, JdbcConvention convention, String database, String schema ) {
        super();
        this.dataSource = Objects.requireNonNull( dataSource );
        this.dialect = Objects.requireNonNull( dialect );
        this.convention = convention;
        this.database = database;
        this.schema = schema;
        this.tableMap = new HashMap<>();
    }


    public JdbcTable createJdbcTable( CatalogCombinedTable combinedTable ) {
        // Temporary type factory, just for the duration of this method. Allowable because we're creating a proto-type, not a type; before being used, the proto-type will be copied into a real type factory.
        final RelDataTypeFactory typeFactory = new SqlTypeFactoryImpl( RelDataTypeSystem.DEFAULT );
        final RelDataTypeFactory.Builder fieldInfo = typeFactory.builder();
        List<String> columnNames = new LinkedList<>();
        for ( CatalogColumn catalogColumn : combinedTable.getColumns() ) {
            SqlTypeName dataTypeName = SqlTypeName.get( catalogColumn.type.name() ); // TODO Replace PolySqlType with native
            RelDataType sqlType = sqlType( typeFactory, dataTypeName, catalogColumn.length, catalogColumn.scale, null );
            fieldInfo.add( catalogColumn.name, sqlType ).nullable( catalogColumn.nullable );
            columnNames.add( catalogColumn.name );
        }
        JdbcTable table = new JdbcTable( this, database, combinedTable.getSchema().name, combinedTable.getTable().name, TableType.valueOf( combinedTable.getTable().tableType.name() ), RelDataTypeImpl.proto( fieldInfo.build() ), columnNames );
        tableMap.put( combinedTable.getTable().name, table );
        return table;
    }


    public static JdbcSchema create( SchemaPlus parentSchema, String name, DataSource dataSource, String catalog, String schema, JdbcPhysicalNameProvider physicalNameProvider ) {
        return create( parentSchema, name, dataSource, SqlDialectFactoryImpl.INSTANCE, catalog, schema, physicalNameProvider );
    }


    public static JdbcSchema create( SchemaPlus parentSchema, String name, DataSource dataSource, SqlDialectFactory dialectFactory, String catalog, String schema, JdbcPhysicalNameProvider physicalNameProvider ) {
        final Expression expression = Schemas.subSchemaExpression( parentSchema, name, JdbcSchema.class );
        final SqlDialect dialect = createDialect( dialectFactory, dataSource );
        final JdbcConvention convention = JdbcConvention.of( dialect, expression, name, physicalNameProvider );
        return new JdbcSchema( dataSource, dialect, convention, catalog, schema );
    }


    // For unit testing only
    public static JdbcSchema create( SchemaPlus parentSchema, String name, DataSource dataSource, String catalog, String schema, ImmutableMap<String, JdbcTable> tableMap, JdbcPhysicalNameProvider physicalNameProvider ) {
        return create( parentSchema, name, dataSource, SqlDialectFactoryImpl.INSTANCE, catalog, schema, tableMap, physicalNameProvider );
    }


    // For unit testing only
    public static JdbcSchema create( SchemaPlus parentSchema, String name, DataSource dataSource, SqlDialectFactory dialectFactory, String catalog, String schema, ImmutableMap<String, JdbcTable> tableMap, JdbcPhysicalNameProvider physicalNameProvider ) {
        final Expression expression = Schemas.subSchemaExpression( parentSchema, name, JdbcSchema.class );
        final SqlDialect dialect = createDialect( dialectFactory, dataSource );
        final JdbcConvention convention = JdbcConvention.of( dialect, expression, name, physicalNameProvider );
        return new JdbcSchema( dataSource, dialect, convention, catalog, schema, tableMap );
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
        return new JdbcSchema( dataSource, dialect, convention, database, schema, tableMap );
    }


    // Used by generated code.
    public DataSource getDataSource() {
        return dataSource;
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

