/*
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
 *
 */

package ch.unibas.dmi.dbis.polyphenydb.jdbc;


import ch.unibas.dmi.dbis.polyphenydb.DataContext;
import ch.unibas.dmi.dbis.polyphenydb.adapter.java.JavaTypeFactory;
import ch.unibas.dmi.dbis.polyphenydb.config.PolyphenyDbConnectionConfig;
import ch.unibas.dmi.dbis.polyphenydb.config.PolyphenyDbConnectionConfigImpl;
import ch.unibas.dmi.dbis.polyphenydb.config.RuntimeConfig;
import ch.unibas.dmi.dbis.polyphenydb.runtime.Hook;
import ch.unibas.dmi.dbis.polyphenydb.runtime.PolyphenyDbContextException;
import ch.unibas.dmi.dbis.polyphenydb.schema.SchemaPlus;
import ch.unibas.dmi.dbis.polyphenydb.schema.SchemaVersion;
import ch.unibas.dmi.dbis.polyphenydb.schema.impl.LongSchemaVersion;
import ch.unibas.dmi.dbis.polyphenydb.sql.SqlExecutableStatement;
import ch.unibas.dmi.dbis.polyphenydb.sql.SqlNode;
import ch.unibas.dmi.dbis.polyphenydb.sql.advise.SqlAdvisor;
import ch.unibas.dmi.dbis.polyphenydb.sql.parser.SqlParser.Config;
import ch.unibas.dmi.dbis.polyphenydb.tools.Planner;
import ch.unibas.dmi.dbis.polyphenydb.tools.RelRunner;
import ch.unibas.dmi.dbis.polyphenydb.util.Holder;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.TimeZone;
import org.apache.calcite.avatica.AvaticaSeverity;
import org.apache.calcite.avatica.AvaticaSite;
import org.apache.calcite.avatica.Meta.ExecuteResult;
import org.apache.calcite.avatica.Meta.MetaResultSet;
import org.apache.calcite.avatica.Meta.StatementHandle;
import org.apache.calcite.avatica.remote.AvaticaRuntimeException;
import org.apache.calcite.linq4j.Linq4j;
import org.apache.calcite.linq4j.QueryProvider;
import org.apache.commons.lang3.time.StopWatch;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


public class DdlExecutionEngine {

    private static final Logger LOG = LoggerFactory.getLogger( DdlExecutionEngine.class );

    private static DdlExecutionEngine INSTANCE;


    static {
        INSTANCE = new DdlExecutionEngine();
    }


    public static DdlExecutionEngine getInstance() {
        return INSTANCE;
    }


    private DdlExecutionEngine() {

    }


    public ExecuteResult execute( final StatementHandle h, final PolyphenyDbStatementHandle statement, final Planner planner, final StopWatch stopWatch, final PolyphenyDbSchema rootSchema, final Config parserConfig, final SqlNode parsed ) {
        if ( parsed instanceof SqlExecutableStatement ) {
            DataContextImpl dataContext = new DataContextImpl( ImmutableMap.of(), rootSchema, parserConfig, "CSV" );
            ContextImpl context = new ContextImpl( rootSchema, dataContext, "C" );

            try {
                ((SqlExecutableStatement) parsed).execute( context );

                // Marshalling
                LinkedList<MetaResultSet> resultSets = new LinkedList<>();
                MetaResultSet resultSet = MetaResultSet.count( statement.getConnection().getConnectionId().toString(), h.id, 1 );
                resultSets.add( resultSet );
                //statement.setOpenResultSet( resultSet );

                return new ExecuteResult( resultSets );

            } catch ( PolyphenyDbContextException e ) { // If there is no exception, everything is fine and the dml query has successfully been executed
                throw new AvaticaRuntimeException( e.getLocalizedMessage(), -1, "", AvaticaSeverity.ERROR );
            }
        } else {
            throw new RuntimeException( "All DDL queries should be of a type that inherits SqlExecutableStatement. But this one is of type " + parsed.getClass() );
        }
    }


    private static class ContextImpl implements PolyphenyDbPrepare.Context {

        private final PolyphenyDbSchema mutableRootSchema;
        private final PolyphenyDbSchema rootSchema;
        private final JavaTypeFactory typeFactory;
        private final DataContext dataContext;
        private final String schemaName;


        ContextImpl( PolyphenyDbSchema rootSchema, DataContext dataContext, String schemaName ) {
            long now = System.currentTimeMillis();
            SchemaVersion schemaVersion = new LongSchemaVersion( now );
            this.mutableRootSchema = rootSchema;
            this.rootSchema = mutableRootSchema.createSnapshot( schemaVersion );
            this.typeFactory = dataContext.getTypeFactory();
            this.dataContext = dataContext;
            this.schemaName = schemaName;
        }


        public JavaTypeFactory getTypeFactory() {
            return typeFactory;
        }


        public PolyphenyDbSchema getRootSchema() {
            return rootSchema;
        }


        public PolyphenyDbSchema getMutableRootSchema() {
            return mutableRootSchema;
        }


        public List<String> getDefaultSchemaPath() {
            return schemaName == null
                    ? ImmutableList.of()
                    : ImmutableList.of( schemaName );
        }


        public List<String> getObjectPath() {
            return null;
        }


        public PolyphenyDbConnectionConfig config() {
            return new PolyphenyDbConnectionConfigImpl( new Properties() );
        }


        public DataContext getDataContext() {
            return dataContext;
        }


        public RelRunner getRelRunner() {
            return null;
        }


        public PolyphenyDbPrepare.SparkHandler spark() {
            final boolean enable = RuntimeConfig.SPARK_ENGINE.getBoolean();
            return PolyphenyDbPrepare.Dummy.getSparkHandler( enable );
        }
    }


    /**
     * Implementation of {@link DataContext} for executing queries without a connection.
     */
    private static class DataContextImpl implements DataContext {

        private final ImmutableMap<Object, Object> map;
        private final PolyphenyDbSchema rootSchema;
        private final QueryProvider queryProvider;
        private final JavaTypeFactory typeFactory;
        private final Config parserConfig;
        private final String schemaName;


        DataContextImpl( Map<String, Object> parameters, PolyphenyDbSchema rootSchema, Config parserConfig, String schemaName ) {
            this.queryProvider = Linq4j.DEFAULT_PROVIDER;
            this.typeFactory = new JavaTypeFactoryImpl();
            this.rootSchema = rootSchema;
            this.parserConfig = parserConfig;
            this.schemaName = schemaName;

            // Store the time at which the query started executing. The SQL standard says that functions such as CURRENT_TIMESTAMP return the same value throughout the query.
            final Holder<Long> timeHolder = Holder.of( System.currentTimeMillis() );

            // Give a hook chance to alter the clock.
            Hook.CURRENT_TIME.run( timeHolder );
            final long time = timeHolder.get();
            final TimeZone timeZone = TimeZone.getDefault();
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
            return null;
            /*
            final List<String> schemaPath =
                    schemaName == null
                            ? ImmutableList.of()
                            : ImmutableList.of( schemaName );
            final SqlValidatorWithHints validator =
                    new SqlAdvisorValidator(
                            SqlStdOperatorTable.instance(),
                            new PolyphenyDbCatalogReader( rootSchema, schemaPath, typeFactory, null ),
                            typeFactory,
                            SqlConformanceEnum.DEFAULT );
            // This duplicates ch.unibas.dmi.dbis.polyphenydb.prepare.PolyphenyDbPrepareImpl.prepare2_
            return new SqlAdvisor( validator, parserConfig );*/
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

}
