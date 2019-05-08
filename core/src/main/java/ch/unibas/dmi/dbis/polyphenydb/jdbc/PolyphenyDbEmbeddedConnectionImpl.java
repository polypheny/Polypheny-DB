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

package ch.unibas.dmi.dbis.polyphenydb.jdbc;


import ch.unibas.dmi.dbis.polyphenydb.DataContext;
import ch.unibas.dmi.dbis.polyphenydb.adapter.java.JavaTypeFactory;
import ch.unibas.dmi.dbis.polyphenydb.config.PolyphenyDbConnectionConfig;
import ch.unibas.dmi.dbis.polyphenydb.config.PolyphenyDbConnectionConfigImpl;
import ch.unibas.dmi.dbis.polyphenydb.jdbc.PolyphenyDbPrepare.Context;
import ch.unibas.dmi.dbis.polyphenydb.jdbc.PolyphenyDbPrepare.PolyphenyDbSignature;
import ch.unibas.dmi.dbis.polyphenydb.materialize.Lattice;
import ch.unibas.dmi.dbis.polyphenydb.materialize.MaterializationService;
import ch.unibas.dmi.dbis.polyphenydb.prepare.PolyphenyDbCatalogReader;
import ch.unibas.dmi.dbis.polyphenydb.rel.type.DelegatingTypeSystem;
import ch.unibas.dmi.dbis.polyphenydb.rel.type.RelDataTypeSystem;
import ch.unibas.dmi.dbis.polyphenydb.runtime.Hook;
import ch.unibas.dmi.dbis.polyphenydb.schema.SchemaPlus;
import ch.unibas.dmi.dbis.polyphenydb.schema.SchemaVersion;
import ch.unibas.dmi.dbis.polyphenydb.schema.Schemas;
import ch.unibas.dmi.dbis.polyphenydb.schema.impl.AbstractSchema;
import ch.unibas.dmi.dbis.polyphenydb.schema.impl.LongSchemaVersion;
import ch.unibas.dmi.dbis.polyphenydb.server.PolyphenyDbServer;
import ch.unibas.dmi.dbis.polyphenydb.server.PolyphenyDbServerStatement;
import ch.unibas.dmi.dbis.polyphenydb.sql.advise.SqlAdvisor;
import ch.unibas.dmi.dbis.polyphenydb.sql.advise.SqlAdvisorValidator;
import ch.unibas.dmi.dbis.polyphenydb.sql.fun.SqlStdOperatorTable;
import ch.unibas.dmi.dbis.polyphenydb.sql.parser.SqlParser;
import ch.unibas.dmi.dbis.polyphenydb.sql.parser.SqlParser.Config;
import ch.unibas.dmi.dbis.polyphenydb.sql.validate.SqlConformanceEnum;
import ch.unibas.dmi.dbis.polyphenydb.sql.validate.SqlValidatorWithHints;
import ch.unibas.dmi.dbis.polyphenydb.tools.RelRunner;
import ch.unibas.dmi.dbis.polyphenydb.util.BuiltInMethod;
import ch.unibas.dmi.dbis.polyphenydb.util.Holder;
import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import java.io.Serializable;
import java.lang.reflect.Type;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Properties;
import java.util.TimeZone;
import java.util.concurrent.atomic.AtomicBoolean;
import org.apache.calcite.avatica.AvaticaConnection;
import org.apache.calcite.avatica.AvaticaFactory;
import org.apache.calcite.avatica.AvaticaSite;
import org.apache.calcite.avatica.AvaticaStatement;
import org.apache.calcite.avatica.Helper;
import org.apache.calcite.avatica.InternalProperty;
import org.apache.calcite.avatica.Meta;
import org.apache.calcite.avatica.MetaImpl;
import org.apache.calcite.avatica.NoSuchStatementException;
import org.apache.calcite.avatica.UnregisteredDriver;
import org.apache.calcite.avatica.remote.TypedValue;
import org.apache.calcite.linq4j.BaseQueryable;
import org.apache.calcite.linq4j.Enumerable;
import org.apache.calcite.linq4j.Enumerator;
import org.apache.calcite.linq4j.Ord;
import org.apache.calcite.linq4j.QueryProvider;
import org.apache.calcite.linq4j.Queryable;
import org.apache.calcite.linq4j.function.Function0;
import org.apache.calcite.linq4j.tree.Expression;
import org.apache.calcite.linq4j.tree.Expressions;


/**
 * Implementation of JDBC connection in the Polypheny-DB engine.
 *
 * Abstract to allow newer versions of JDBC to add methods.
 */
abstract class PolyphenyDbEmbeddedConnectionImpl extends AvaticaConnection implements PolyphenyDbEmbeddedConnection, QueryProvider {

    public final JavaTypeFactory typeFactory;

    final PolyphenyDbSchema rootSchema;
    final Function0<PolyphenyDbPrepare> prepareFactory;
    final PolyphenyDbServer server = new PolyphenyDbServerImpl();

    // must be package-protected
    static final Trojan TROJAN = createTrojan();


    /**
     * Creates a PolyphenyDbEmbeddedConnectionImpl.
     *
     * Not public; method is called only from the embeddedDriver.
     *
     * @param embeddedDriver EmbeddedDriver
     * @param factory Factory for JDBC objects
     * @param url Server URL
     * @param info Other connection properties
     * @param rootSchema Root schema, or null
     * @param typeFactory Type factory, or null
     */
    protected PolyphenyDbEmbeddedConnectionImpl( EmbeddedDriver embeddedDriver, AvaticaFactory factory, String url, Properties info, PolyphenyDbSchema rootSchema, JavaTypeFactory typeFactory ) {
        super( embeddedDriver, factory, url, info );
        PolyphenyDbConnectionConfig cfg = new PolyphenyDbConnectionConfigImpl( info );
        this.prepareFactory = embeddedDriver.prepareFactory;
        if ( typeFactory != null ) {
            this.typeFactory = typeFactory;
        } else {
            RelDataTypeSystem typeSystem = cfg.typeSystem( RelDataTypeSystem.class, RelDataTypeSystem.DEFAULT );
            if ( cfg.conformance().shouldConvertRaggedUnionTypesToVarying() ) {
                typeSystem = new DelegatingTypeSystem( typeSystem ) {
                    @Override
                    public boolean shouldConvertRaggedUnionTypesToVarying() {
                        return true;
                    }
                };
            }
            this.typeFactory = new JavaTypeFactoryImpl( typeSystem );
        }
        this.rootSchema =
                Objects.requireNonNull( rootSchema != null
                        ? rootSchema
                        : PolyphenyDbSchema.createRootSchema( true ) );
        Preconditions.checkArgument( this.rootSchema.isRoot(), "must be root schema" );
        this.properties.put( InternalProperty.CASE_SENSITIVE, cfg.caseSensitive() );
        this.properties.put( InternalProperty.UNQUOTED_CASING, cfg.unquotedCasing() );
        this.properties.put( InternalProperty.QUOTED_CASING, cfg.quotedCasing() );
        this.properties.put( InternalProperty.QUOTING, cfg.quoting() );
    }


    PolyphenyDbEmbeddedMetaImpl meta() {
        return (PolyphenyDbEmbeddedMetaImpl) meta;
    }


    public PolyphenyDbConnectionConfig config() {
        return new PolyphenyDbConnectionConfigImpl( info );
    }


    public Context createPrepareContext() {
        return new ContextImpl( this );
    }


    /**
     * Called after the constructor has completed and the model has been loaded.
     */
    void init() {
        final MaterializationService service = MaterializationService.instance();
        for ( PolyphenyDbSchema.LatticeEntry e : Schemas.getLatticeEntries( rootSchema ) ) {
            final Lattice lattice = e.getLattice();
            for ( Lattice.Tile tile : lattice.computeTiles() ) {
                service.defineTile( lattice, tile.bitSet(), tile.measures, e.schema, true, true );
            }
        }
    }


    @Override
    public <T> T unwrap( Class<T> iface ) throws SQLException {
        if ( iface == RelRunner.class ) {
            return iface.cast( (RelRunner) rel -> {
                try {
                    return prepareStatement_( PolyphenyDbPrepare.Query.of( rel ), ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_READ_ONLY, getHoldability() );
                } catch ( SQLException e ) {
                    throw new RuntimeException( e );
                }
            } );
        }
        return super.unwrap( iface );
    }


    @Override
    public PolyphenyDbEmbeddedStatement createStatement( int resultSetType, int resultSetConcurrency, int resultSetHoldability ) throws SQLException {
        return (PolyphenyDbEmbeddedStatement) super.createStatement( resultSetType, resultSetConcurrency, resultSetHoldability );
    }


    @Override
    public PolyphenyDbEmbeddedPreparedStatement prepareStatement( String sql, int resultSetType, int resultSetConcurrency, int resultSetHoldability ) throws SQLException {
        final PolyphenyDbPrepare.Query<Object> query = PolyphenyDbPrepare.Query.of( sql );
        return prepareStatement_( query, resultSetType, resultSetConcurrency, resultSetHoldability );
    }


    private PolyphenyDbEmbeddedPreparedStatement prepareStatement_( PolyphenyDbPrepare.Query<?> query, int resultSetType, int resultSetConcurrency, int resultSetHoldability ) throws SQLException {
        try {
            final Meta.Signature signature = parseQuery( query, createPrepareContext(), -1 );
            final PolyphenyDbEmbeddedPreparedStatement polyphenyDbEmbeddedPreparedStatement = (PolyphenyDbEmbeddedPreparedStatement) factory.newPreparedStatement( this, null, signature, resultSetType, resultSetConcurrency, resultSetHoldability );
            server.getStatement( polyphenyDbEmbeddedPreparedStatement.handle ).setSignature( signature );
            return polyphenyDbEmbeddedPreparedStatement;
        } catch ( Exception e ) {
            throw Helper.INSTANCE.createException( "Error while preparing statement [" + query.sql + "]", e );
        }
    }


    <T> PolyphenyDbSignature<T> parseQuery( PolyphenyDbPrepare.Query<T> query, PolyphenyDbPrepare.Context prepareContext, long maxRowCount ) {
        PolyphenyDbPrepare.Dummy.push( prepareContext );
        try {
            final PolyphenyDbPrepare prepare = prepareFactory.apply();
            return prepare.prepareSql( prepareContext, query, Object[].class, maxRowCount );
        } finally {
            PolyphenyDbPrepare.Dummy.pop( prepareContext );
        }
    }


    @Override
    public AtomicBoolean getCancelFlag( Meta.StatementHandle handle ) throws NoSuchStatementException {
        final PolyphenyDbServerStatement serverStatement = server.getStatement( handle );
        return ((PolyphenyDbServerStatementImpl) serverStatement).cancelFlag;
    }


    public SchemaPlus getRootSchema() {
        return rootSchema.plus();
    }


    public JavaTypeFactory getTypeFactory() {
        return typeFactory;
    }


    public Properties getProperties() {
        return info;
    }


    public <T> Queryable<T> createQuery( Expression expression, Class<T> rowType ) {
        return new PolyphenyDbQueryable<>( this, rowType, expression );
    }


    public <T> Queryable<T> createQuery( Expression expression, Type rowType ) {
        return new PolyphenyDbQueryable<>( this, rowType, expression );
    }


    public <T> T execute( Expression expression, Type type ) {
        return null; // TODO:
    }


    public <T> T execute( Expression expression, Class<T> type ) {
        return null; // TODO:
    }


    public <T> Enumerator<T> executeQuery( Queryable<T> queryable ) {
        try {
            PolyphenyDbEmbeddedStatement statement = (PolyphenyDbEmbeddedStatement) createStatement();
            PolyphenyDbSignature<T> signature = statement.prepare( queryable );
            return enumerable( statement.handle, signature ).enumerator();
        } catch ( SQLException e ) {
            throw new RuntimeException( e );
        }
    }


    public <T> Enumerable<T> enumerable( Meta.StatementHandle handle, PolyphenyDbSignature<T> signature ) throws SQLException {
        Map<String, Object> map = new LinkedHashMap<>();
        AvaticaStatement statement = lookupStatement( handle );
        final List<TypedValue> parameterValues = TROJAN.getParameterValues( statement );

        if ( MetaImpl.checkParameterValueHasNull( parameterValues ) ) {
            throw new SQLException( "exception while executing query: unbound parameter" );
        }

        for ( Ord<TypedValue> o : Ord.zip( parameterValues ) ) {
            map.put( "?" + o.i, o.e.toLocal() );
        }
        map.putAll( signature.internalParameters );
        final AtomicBoolean cancelFlag;
        try {
            cancelFlag = getCancelFlag( handle );
        } catch ( NoSuchStatementException e ) {
            throw new RuntimeException( e );
        }
        map.put( DataContext.Variable.CANCEL_FLAG.camelName, cancelFlag );
        int queryTimeout = statement.getQueryTimeout();
        // Avoid overflow
        if ( queryTimeout > 0 && queryTimeout < Integer.MAX_VALUE / 1000 ) {
            map.put( DataContext.Variable.TIMEOUT.camelName, queryTimeout * 1000L );
        }
        final DataContext dataContext = createDataContext( map, signature.rootSchema );
        return signature.enumerable( dataContext );
    }


    public DataContext createDataContext( Map<String, Object> parameterValues, PolyphenyDbSchema rootSchema ) {
        if ( config().spark() ) {
            return new SlimDataContext();
        }
        return new DataContextImpl( this, parameterValues, rootSchema );
    }


    // do not make public
    UnregisteredDriver getDriver() {
        return driver;
    }


    // do not make public
    AvaticaFactory getFactory() {
        return factory;
    }


    /**
     * Implementation of Queryable.
     *
     * @param <T> element type
     */
    static class PolyphenyDbQueryable<T> extends BaseQueryable<T> {

        PolyphenyDbQueryable( PolyphenyDbEmbeddedConnection connection, Type elementType, Expression expression ) {
            super( connection, elementType, expression );
        }


        public PolyphenyDbEmbeddedConnection getConnection() {
            return (PolyphenyDbEmbeddedConnection) provider;
        }
    }


    /**
     * Implementation of Server.
     */
    private static class PolyphenyDbServerImpl implements PolyphenyDbServer {

        final Map<Integer, PolyphenyDbServerStatement> statementMap = new HashMap<>();


        public void removeStatement( Meta.StatementHandle h ) {
            statementMap.remove( h.id );
        }


        public void addStatement( PolyphenyDbEmbeddedConnection connection, Meta.StatementHandle h ) {
            final PolyphenyDbEmbeddedConnectionImpl c = (PolyphenyDbEmbeddedConnectionImpl) connection;
            final PolyphenyDbServerStatement previous = statementMap.put( h.id, new PolyphenyDbServerStatementImpl( c ) );
            if ( previous != null ) {
                throw new AssertionError();
            }
        }


        public PolyphenyDbServerStatement getStatement( Meta.StatementHandle h ) throws NoSuchStatementException {
            PolyphenyDbServerStatement statement = statementMap.get( h.id );
            if ( statement == null ) {
                throw new NoSuchStatementException( h );
            }
            return statement;
        }
    }


    /**
     * Schema that has no parents.
     */
    static class RootSchema extends AbstractSchema {

        RootSchema() {
            super();
        }


        @Override
        public Expression getExpression( SchemaPlus parentSchema, String name ) {
            return Expressions.call( DataContext.ROOT, BuiltInMethod.DATA_CONTEXT_GET_ROOT_SCHEMA.method );
        }
    }


    /**
     * Implementation of DataContext.
     */
    static class DataContextImpl implements DataContext {

        private final ImmutableMap<Object, Object> map;
        private final PolyphenyDbSchema rootSchema;
        private final QueryProvider queryProvider;
        private final JavaTypeFactory typeFactory;


        DataContextImpl( PolyphenyDbEmbeddedConnectionImpl connection, Map<String, Object> parameters, PolyphenyDbSchema rootSchema ) {
            this.queryProvider = connection;
            this.typeFactory = connection.getTypeFactory();
            this.rootSchema = rootSchema;

            // Store the time at which the query started executing. The SQL standard says that functions such as CURRENT_TIMESTAMP return the same value throughout the query.
            final Holder<Long> timeHolder = Holder.of( System.currentTimeMillis() );

            // Give a hook chance to alter the clock.
            Hook.CURRENT_TIME.run( timeHolder );
            final long time = timeHolder.get();
            final TimeZone timeZone = connection.getTimeZone();
            final long localOffset = timeZone.getOffset( time );
            final long currentOffset = localOffset;

            // Give a hook chance to alter standard input, output, error streams.
            final Holder<Object[]> streamHolder = Holder.of( new Object[]{ System.in, System.out, System.err } );
            Hook.STANDARD_STREAMS.run( streamHolder );

            ImmutableMap.Builder<Object, Object> builder = ImmutableMap.builder();
            builder.put( Variable.UTC_TIMESTAMP.camelName, time )
                    .put( Variable.CURRENT_TIMESTAMP.camelName, time + currentOffset )
                    .put( Variable.LOCAL_TIMESTAMP.camelName, time + localOffset )
                    .put( Variable.TIME_ZONE.camelName, timeZone )
                    .put( Variable.STDIN.camelName, streamHolder.get()[0] )
                    .put( Variable.STDOUT.camelName, streamHolder.get()[1] )
                    .put( Variable.STDERR.camelName, streamHolder.get()[2] );
            for ( Map.Entry<String, Object> entry : parameters.entrySet() ) {
                Object e = entry.getValue();
                if ( e == null ) {
                    e = AvaticaSite.DUMMY_VALUE;
                }
                builder.put( entry.getKey(), e );
            }
            map = builder.build();
        }


        public synchronized Object get( String name ) {
            Object o = map.get( name );
            if ( o == AvaticaSite.DUMMY_VALUE ) {
                return null;
            }
            if ( o == null && Variable.SQL_ADVISOR.camelName.equals( name ) ) {
                return getSqlAdvisor();
            }
            return o;
        }


        private SqlAdvisor getSqlAdvisor() {
            final PolyphenyDbEmbeddedConnectionImpl con = (PolyphenyDbEmbeddedConnectionImpl) queryProvider;
            final String schemaName;
            try {
                schemaName = con.getSchema();
            } catch ( SQLException e ) {
                throw new RuntimeException( e );
            }
            final List<String> schemaPath =
                    schemaName == null
                            ? ImmutableList.of()
                            : ImmutableList.of( schemaName );
            final SqlValidatorWithHints validator =
                    new SqlAdvisorValidator(
                            SqlStdOperatorTable.instance(),
                            new PolyphenyDbCatalogReader( rootSchema, schemaPath, typeFactory, con.config() ), typeFactory, SqlConformanceEnum.DEFAULT );
            final PolyphenyDbConnectionConfig config = con.config();
            // This duplicates ch.unibas.dmi.dbis.polyphenydb.prepare.PolyphenyDbPrepareImpl.prepare2_
            final Config parserConfig = SqlParser.configBuilder()
                    .setQuotedCasing( config.quotedCasing() )
                    .setUnquotedCasing( config.unquotedCasing() )
                    .setQuoting( config.quoting() )
                    .setConformance( config.conformance() )
                    .setCaseSensitive( config.caseSensitive() )
                    .build();
            return new SqlAdvisor( validator, parserConfig );
        }


        public SchemaPlus getRootSchema() {
            return rootSchema == null ? null : rootSchema.plus();
        }


        public JavaTypeFactory getTypeFactory() {
            return typeFactory;
        }


        public QueryProvider getQueryProvider() {
            return queryProvider;
        }
    }


    /**
     * Implementation of Context.
     */
    static class ContextImpl implements PolyphenyDbPrepare.Context {

        private final PolyphenyDbEmbeddedConnectionImpl connection;
        private final PolyphenyDbSchema mutableRootSchema;
        private final PolyphenyDbSchema rootSchema;


        ContextImpl( PolyphenyDbEmbeddedConnectionImpl connection ) {
            this.connection = Objects.requireNonNull( connection );
            long now = System.currentTimeMillis();
            SchemaVersion schemaVersion = new LongSchemaVersion( now );
            this.mutableRootSchema = connection.rootSchema;
            this.rootSchema = mutableRootSchema.createSnapshot( schemaVersion );
        }


        public JavaTypeFactory getTypeFactory() {
            return connection.typeFactory;
        }


        public PolyphenyDbSchema getRootSchema() {
            return rootSchema;
        }


        public PolyphenyDbSchema getMutableRootSchema() {
            return mutableRootSchema;
        }


        public List<String> getDefaultSchemaPath() {
            final String schemaName;
            try {
                schemaName = connection.getSchema();
            } catch ( SQLException e ) {
                throw new RuntimeException( e );
            }
            return schemaName == null
                    ? ImmutableList.of()
                    : ImmutableList.of( schemaName );
        }


        public List<String> getObjectPath() {
            return null;
        }


        public PolyphenyDbConnectionConfig config() {
            return connection.config();
        }


        public DataContext getDataContext() {
            return connection.createDataContext( ImmutableMap.of(), rootSchema );
        }


        public RelRunner getRelRunner() {
            final RelRunner runner;
            try {
                runner = connection.unwrap( RelRunner.class );
            } catch ( SQLException e ) {
                throw new RuntimeException( e );
            }
            if ( runner == null ) {
                throw new UnsupportedOperationException();
            }
            return runner;
        }


        public PolyphenyDbPrepare.SparkHandler spark() {
            final boolean enable = config().spark();
            return PolyphenyDbPrepare.Dummy.getSparkHandler( enable );
        }
    }


    /**
     * Implementation of {@link DataContext} that has few variables and is {@link Serializable}. For Spark.
     */
    private static class SlimDataContext implements DataContext, Serializable {

        public SchemaPlus getRootSchema() {
            return null;
        }


        public JavaTypeFactory getTypeFactory() {
            return null;
        }


        public QueryProvider getQueryProvider() {
            return null;
        }


        public Object get( String name ) {
            return null;
        }
    }


    /**
     * Implementation of {@link PolyphenyDbServerStatement}.
     */
    static class PolyphenyDbServerStatementImpl implements PolyphenyDbServerStatement {

        private final PolyphenyDbEmbeddedConnectionImpl connection;
        private Iterator<Object> iterator;
        private Meta.Signature signature;
        private final AtomicBoolean cancelFlag = new AtomicBoolean();


        PolyphenyDbServerStatementImpl( PolyphenyDbEmbeddedConnectionImpl connection ) {
            this.connection = Objects.requireNonNull( connection );
        }


        public Context createPrepareContext() {
            return connection.createPrepareContext();
        }


        public PolyphenyDbEmbeddedConnection getConnection() {
            return connection;
        }


        public void setSignature( Meta.Signature signature ) {
            this.signature = signature;
        }


        public Meta.Signature getSignature() {
            return signature;
        }


        public Iterator<Object> getResultSet() {
            return iterator;
        }


        public void setResultSet( Iterator<Object> iterator ) {
            this.iterator = iterator;
        }
    }

}
