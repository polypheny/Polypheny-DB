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
 */

package org.polypheny.db.adapter.cassandra;


import com.datastax.oss.driver.api.core.CqlSession;
import com.datastax.oss.driver.api.core.cql.ResultSet;
import lombok.extern.slf4j.Slf4j;
import org.apache.calcite.linq4j.AbstractEnumerable;
import org.apache.calcite.linq4j.Enumerator;
import org.apache.calcite.linq4j.Linq4j;


@Slf4j
public class CassandraEnumerable extends AbstractEnumerable<Object> {

    final CqlSession session;
    final String stringStatement;
    final Integer offset;


    public CassandraEnumerable( CqlSession session, String statement, Integer offset ) {
        this.session = session;
        this.stringStatement = statement;
        this.offset = offset;
    }


    public CassandraEnumerable( CqlSession session, String statement ) {
        this( session, statement, 0 );
    }


    public static CassandraEnumerable of( CqlSession session, String statement ) {
        return CassandraEnumerable.of( session, statement, 0 );
    }


    public static CassandraEnumerable of( CqlSession session, String statement, Integer offset ) {
        log.debug( "Creating string enumerable with: {}, offset: {}", statement, offset );
        return new CassandraEnumerable( session, statement );
    }


    @Override
    public Enumerator<Object> enumerator() {

        final ResultSet results = session.execute( this.stringStatement );
        // Skip results until we get to the right offset
        if ( results.getColumnDefinitions().size() == 0 ) {
            return Linq4j.singletonEnumerator( (Object) 0 );
        }
        int skip = 0;
        Enumerator<Object> enumerator = new CassandraEnumerator( results );
        while ( skip < offset && enumerator.moveNext() ) {
            skip++;
        }
        return enumerator;
    }

}
