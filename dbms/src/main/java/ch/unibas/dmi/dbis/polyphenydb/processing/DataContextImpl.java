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

package ch.unibas.dmi.dbis.polyphenydb.processing;


import ch.unibas.dmi.dbis.polyphenydb.DataContext;
import ch.unibas.dmi.dbis.polyphenydb.Transaction;
import ch.unibas.dmi.dbis.polyphenydb.adapter.java.JavaTypeFactory;
import ch.unibas.dmi.dbis.polyphenydb.runtime.Hook;
import ch.unibas.dmi.dbis.polyphenydb.schema.PolyphenyDbSchema;
import ch.unibas.dmi.dbis.polyphenydb.schema.SchemaPlus;
import ch.unibas.dmi.dbis.polyphenydb.util.Holder;
import com.google.common.collect.ImmutableMap;
import java.util.Map;
import java.util.TimeZone;
import lombok.Getter;
import org.apache.calcite.avatica.AvaticaSite;
import org.apache.calcite.linq4j.QueryProvider;



/**
 * Implementation of DataContext.
 */
public class DataContextImpl implements DataContext {

    private final ImmutableMap<Object, Object> map;
    private final PolyphenyDbSchema rootSchema;
    private final QueryProvider queryProvider;
    private final JavaTypeFactory typeFactory;
    private final TimeZone timeZone = TimeZone.getDefault();
    @Getter
    private final Transaction transaction;


    public DataContextImpl( QueryProvider queryProvider, Map<String, Object> parameters, PolyphenyDbSchema rootSchema, JavaTypeFactory typeFactory, Transaction transaction ) {
        this.queryProvider = queryProvider;
        this.typeFactory = typeFactory;
        this.rootSchema = rootSchema;
        this.transaction = transaction;
        
        // Store the time at which the query started executing. The SQL standard says that functions such as CURRENT_TIMESTAMP return the same value throughout the query.
        final Holder<Long> timeHolder = Holder.of( System.currentTimeMillis() );

        // Give a hook chance to alter the clock.
        Hook.CURRENT_TIME.run( timeHolder );
        final long time = timeHolder.get();
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


    @Override
    public synchronized Object get( String name ) {
        Object o = map.get( name );
        if ( o == AvaticaSite.DUMMY_VALUE ) {
            return null;
        }
        /* if ( o == null && Variable.SQL_ADVISOR.camelName.equals( name ) ) {
            return getSqlAdvisor();
        } */
        return o;
    }

/*
    private SqlAdvisor getSqlAdvisor() {
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
                        new PolyphenyDbCatalogReader( rootSchema, schemaPath, typeFactory ), typeFactory, SqlConformanceEnum.DEFAULT );
        final PolyphenyDbConnectionConfig config = con.config();
        // This duplicates ch.unibas.dmi.dbis.polyphenydb.prepare.PolyphenyDbPrepareImpl.prepare2_
        final Config parserConfig = SqlParser.configBuilder()
                .setQuotedCasing( config.quotedCasing() )
                .setUnquotedCasing( config.unquotedCasing() )
                .setQuoting( config.quoting() )
                .setConformance( config.conformance() )
                .setCaseSensitive( RuntimeConfig.CASE_SENSITIVE.getBoolean() )
                .build();
        return new SqlAdvisor( validator, parserConfig );
    }
*/


    @Override
    public SchemaPlus getRootSchema() {
        return rootSchema == null ? null : rootSchema.plus();
    }


    @Override
    public JavaTypeFactory getTypeFactory() {
        return typeFactory;
    }


    @Override
    public QueryProvider getQueryProvider() {
        return queryProvider;
    }

}
