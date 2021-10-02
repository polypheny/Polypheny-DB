/*
 * Copyright 2019-2021 The Polypheny Project
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
 */

package org.polypheny.db.processing;


import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.TimeZone;
import java.util.stream.Collectors;
import lombok.Getter;
import org.apache.calcite.avatica.AvaticaSite;
import org.apache.calcite.linq4j.QueryProvider;
import org.polypheny.db.adapter.DataContext;
import org.polypheny.db.adapter.java.JavaTypeFactory;
import org.polypheny.db.rel.type.RelDataType;
import org.polypheny.db.runtime.Hook;
import org.polypheny.db.schema.PolyphenyDbSchema;
import org.polypheny.db.schema.SchemaPlus;
import org.polypheny.db.transaction.Statement;
import org.polypheny.db.util.Holder;


/**
 * Implementation of DataContext.
 */
public class DataContextImpl implements DataContext {

    private final HashMap<String, Object> map;
    private final PolyphenyDbSchema rootSchema;
    private final QueryProvider queryProvider;
    private final JavaTypeFactory typeFactory;
    private final TimeZone timeZone = TimeZone.getDefault();
    @Getter
    private final Statement statement;

    private boolean wasBackuped = false;

    private final Map<Long, RelDataType> parameterTypes; // ParameterIndex -> Data Type
    private List<Map<Long, Object>> parameterValues; // List of ( ParameterIndex -> Value )

    private Map<Long, RelDataType> backupParameterTypes = new HashMap<>(); // ParameterIndex -> Data Type
    private List<Map<Long, Object>> backupParameterValues = new ArrayList<>(); // List of ( ParameterIndex -> Value )


    public DataContextImpl( QueryProvider queryProvider, Map<String, Object> parameters, PolyphenyDbSchema rootSchema, JavaTypeFactory typeFactory, Statement statement ) {
        this.queryProvider = queryProvider;
        this.typeFactory = typeFactory;
        this.rootSchema = rootSchema;
        this.statement = statement;

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

        map = new HashMap<>();
        map.put( Variable.UTC_TIMESTAMP.camelName, time );
        map.put( Variable.CURRENT_TIMESTAMP.camelName, time + currentOffset );
        map.put( Variable.LOCAL_TIMESTAMP.camelName, time + localOffset );
        map.put( Variable.TIME_ZONE.camelName, timeZone );
        map.put( Variable.STDIN.camelName, streamHolder.get()[0] );
        map.put( Variable.STDOUT.camelName, streamHolder.get()[1] );
        map.put( Variable.STDERR.camelName, streamHolder.get()[2] );
        for ( Map.Entry<String, Object> entry : parameters.entrySet() ) {
            Object e = entry.getValue();
            if ( e == null ) {
                e = AvaticaSite.DUMMY_VALUE;
            }
            map.put( entry.getKey(), e );
        }

        parameterTypes = new HashMap<>();
        parameterValues = new LinkedList<>();
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


    @Override
    public void addAll( Map<String, Object> map ) {
        this.map.putAll( map );
    }


    @Override
    public void addParameterValues( long index, RelDataType type, List<Object> data ) {
        if ( parameterTypes.containsKey( index ) ) {
            throw new RuntimeException( "There are already values assigned to this index" );
        }
        if ( parameterValues.size() == 0 ) {
            for ( Object d : data ) {
                parameterValues.add( new HashMap<>() );
            }
        }
        if ( parameterValues.size() != data.size() ) {
            throw new RuntimeException( "Expecting " + parameterValues.size() + " rows but " + data.size() + " values specified!" );
        }
        parameterTypes.put( index, type );
        int i = 0;
        for ( Object d : data ) {
            parameterValues.get( i++ ).put( index, d );
        }
    }


    @Override
    public RelDataType getParameterType( long index ) {
        return parameterTypes.get( index );
    }


    @Override
    public List<Map<Long, Object>> getParameterValues() {
        return parameterValues;
    }


    @Override
    public void resetParameterValues() {

        parameterTypes.clear();
        parameterValues.clear();
    }


    @Override
    public boolean wasBackuped() {
        return wasBackuped;
    }


    @Override
    public void backupParameterValues() {

        wasBackuped = true;

        backupParameterTypes.putAll( parameterTypes );
        backupParameterValues = parameterValues.stream().collect( Collectors.toList() );
    }


    @Override
    public void restoreParameterValues() {

        parameterTypes.putAll( backupParameterTypes );
        parameterValues = backupParameterValues.stream().collect( Collectors.toList() );

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
        // This duplicates org.polypheny.db.prepare.PolyphenyDbPrepareImpl.prepare2_
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
