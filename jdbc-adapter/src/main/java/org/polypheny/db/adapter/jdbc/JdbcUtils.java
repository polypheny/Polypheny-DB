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


import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.LoadingCache;
import com.google.common.collect.ImmutableList;
import com.google.common.primitives.Ints;
import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.Date;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Time;
import java.sql.Timestamp;
import java.sql.Types;
import java.util.Arrays;
import java.util.HashMap;
import java.util.IdentityHashMap;
import java.util.List;
import java.util.Map;
import java.util.TimeZone;
import javax.annotation.Nonnull;
import javax.sql.DataSource;
import org.apache.calcite.avatica.ColumnMetaData;
import org.apache.calcite.avatica.util.DateTimeUtils;
import org.apache.calcite.linq4j.function.Function0;
import org.apache.calcite.linq4j.function.Function1;
import org.apache.commons.dbcp2.BasicDataSource;
import org.polypheny.db.adapter.jdbc.connection.ConnectionFactory;
import org.polypheny.db.information.Information;
import org.polypheny.db.information.InformationGraph;
import org.polypheny.db.information.InformationGraph.GraphData;
import org.polypheny.db.information.InformationGraph.GraphType;
import org.polypheny.db.information.InformationGroup;
import org.polypheny.db.information.InformationPage;
import org.polypheny.db.information.InformationTable;
import org.polypheny.db.sql.language.SqlDialect;
import org.polypheny.db.sql.language.SqlDialectFactory;
import org.polypheny.db.util.ImmutableNullableList;
import org.polypheny.db.util.Pair;


/**
 * Utilities for the JDBC provider.
 */
public final class JdbcUtils {

    private JdbcUtils() {
        throw new AssertionError( "no instances!" );
    }


    /**
     * Pool of dialects.
     */
    static class DialectPool {

        final Map<DataSource, Map<SqlDialectFactory, SqlDialect>> map0 = new IdentityHashMap<>();
        final Map<List, SqlDialect> map = new HashMap<>();

        public static final DialectPool INSTANCE = new DialectPool();


        // TODO: Discuss why we need a pool. If we do, I'd like to improve performance
        synchronized SqlDialect get( SqlDialectFactory dialectFactory, DataSource dataSource ) {
            Map<SqlDialectFactory, SqlDialect> dialectMap = map0.get( dataSource );
            if ( dialectMap != null ) {
                final SqlDialect sqlDialect = dialectMap.get( dialectFactory );
                if ( sqlDialect != null ) {
                    return sqlDialect;
                }
            }
            Connection connection = null;
            try {
                connection = dataSource.getConnection();
                DatabaseMetaData metaData = connection.getMetaData();
                String productName = metaData.getDatabaseProductName();
                String productVersion = metaData.getDatabaseProductVersion();
                List key = ImmutableList.of( productName, productVersion, dialectFactory );
                SqlDialect dialect = map.get( key );
                if ( dialect == null ) {
                    dialect = dialectFactory.create( metaData );
                    map.put( key, dialect );
                    if ( dialectMap == null ) {
                        dialectMap = new IdentityHashMap<>();
                        map0.put( dataSource, dialectMap );
                    }
                    dialectMap.put( dialectFactory, dialect );
                }
                connection.close();
                connection = null;
                return dialect;
            } catch ( SQLException e ) {
                throw new RuntimeException( e );
            } finally {
                if ( connection != null ) {
                    try {
                        connection.close();
                    } catch ( SQLException e ) {
                        // ignore
                    }
                }
            }
        }

    }


    /**
     * Builder that calls {@link ResultSet#getObject(int)} for every column, or {@code getXxx} if the result type
     * is a primitive {@code xxx}, and returns an array of objects for each row.
     */
    static class ObjectArrayRowBuilder implements Function0<Object[]> {

        private final ResultSet resultSet;
        private final int columnCount;
        private final ColumnMetaData.Rep[] reps;
        private final int[] types;


        ObjectArrayRowBuilder( ResultSet resultSet, ColumnMetaData.Rep[] reps, int[] types ) throws SQLException {
            this.resultSet = resultSet;
            this.reps = reps;
            this.types = types;
            this.columnCount = resultSet.getMetaData().getColumnCount();
        }


        public static Function1<ResultSet, Function0<Object[]>> factory( final List<Pair<ColumnMetaData.Rep, Integer>> list ) {
            return resultSet -> {
                try {
                    return new ObjectArrayRowBuilder(
                            resultSet,
                            Pair.left( list ).toArray( new ColumnMetaData.Rep[list.size()] ),
                            Ints.toArray( Pair.right( list ) ) );
                } catch ( SQLException e ) {
                    throw new RuntimeException( e );
                }
            };
        }


        @Override
        public Object[] apply() {
            try {
                final Object[] values = new Object[columnCount];
                for ( int i = 0; i < columnCount; i++ ) {
                    values[i] = value( i );
                }
                return values;
            } catch ( SQLException e ) {
                throw new RuntimeException( e );
            }
        }


        /**
         * Gets a value from a given column in a JDBC result set.
         *
         * @param i Ordinal of column (1-based, per JDBC)
         */
        private Object value( int i ) throws SQLException {
            // MySQL returns timestamps shifted into local time. Using getTimestamp(int, Calendar) with a UTC calendar
            // should prevent this, but does not. So we shift explicitly.
            switch ( types[i] ) {
                case Types.TIMESTAMP:
                    return shift( resultSet.getTimestamp( i + 1 ) );
                case Types.TIME:
                    return shift( resultSet.getTime( i + 1 ) );
                case Types.DATE:
                    return shift( resultSet.getDate( i + 1 ) );
            }
            return reps[i].jdbcGet( resultSet, i + 1 );
        }


        private static Timestamp shift( Timestamp v ) {
            if ( v == null ) {
                return null;
            }
            long time = v.getTime();
            int offset = TimeZone.getDefault().getOffset( time );
            return new Timestamp( time + offset );
        }


        private static Time shift( Time v ) {
            if ( v == null ) {
                return null;
            }
            long time = v.getTime();
            int offset = TimeZone.getDefault().getOffset( time );
            return new Time( (time + offset) % DateTimeUtils.MILLIS_PER_DAY );
        }


        private static Date shift( Date v ) {
            if ( v == null ) {
                return null;
            }
            long time = v.getTime();
            int offset = TimeZone.getDefault().getOffset( time );
            return new Date( time + offset );
        }

    }


    /**
     * Ensures that if two data sources have the same definition, they will use the same object.
     *
     * This in turn makes it easier to cache {@link SqlDialect} objects. Otherwise, each time we see a new data source,
     * we have to open a connection to find out what database product and version it is.
     */
    public static class DataSourcePool {

        public static final DataSourcePool INSTANCE = new DataSourcePool();

        private final LoadingCache<List<String>, BasicDataSource> cache = CacheBuilder.newBuilder().softValues().build( CacheLoader.from( DataSourcePool::dataSource ) );


        private static @Nonnull
        BasicDataSource dataSource( @Nonnull List<String> key ) {
            BasicDataSource dataSource = new BasicDataSource();
            dataSource.setUrl( key.get( 0 ) );
            dataSource.setUsername( key.get( 1 ) );
            dataSource.setPassword( key.get( 2 ) );
            dataSource.setDriverClassName( key.get( 3 ) );
            return dataSource;
        }


        public DataSource get( String url, String driverClassName, String username, String password ) {
            // Get data source objects from a cache, so that we don't have to sniff out what kind of
            // database they are quite as often.
            final List<String> key = ImmutableNullableList.of( url, username, password, driverClassName );
            return cache.getUnchecked( key );
        }

    }


    /**
     * Builds and adds an new information group, observing the connection pool, to the provided information objects
     *
     * @param informationPage The information page used to show information on this jdbc adapter
     * @param groups The collection of information groups associated with this adapter
     * @param informationElements The collection of information elements associated with this adapter
     */
    public static void addInformationPoolSize( InformationPage informationPage, List<InformationGroup> groups, List<Information> informationElements, ConnectionFactory connectionFactory, String uniqueName ) {
        InformationGroup group = new InformationGroup( informationPage, "JDBC Connection Pool" );

        InformationGraph connectionPoolSizeGraph = new InformationGraph(
                group,
                GraphType.DOUGHNUT,
                new String[]{ "Active", "Available", "Idle" }
        );
        informationElements.add( connectionPoolSizeGraph );

        InformationTable connectionPoolSizeTable = new InformationTable(
                group,
                Arrays.asList( "Attribute", "Value" ) );
        informationElements.add( connectionPoolSizeTable );

        group.setRefreshFunction( () -> {
            int idle = connectionFactory.getNumIdle();
            int active = connectionFactory.getNumActive();
            int max = connectionFactory.getMaxTotal();
            int available = max - idle - active;

            connectionPoolSizeGraph.updateGraph(
                    new String[]{ "Active", "Available", "Idle" },
                    new GraphData<>( uniqueName + "-connection-pool-data", new Integer[]{ active, available, idle } )
            );

            connectionPoolSizeTable.reset();
            connectionPoolSizeTable.addRow( "Active", active );
            connectionPoolSizeTable.addRow( "Idle", idle );
            connectionPoolSizeTable.addRow( "Max", max );
        } );

        groups.add( group );

    }

}

