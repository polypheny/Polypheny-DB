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
 */

package org.polypheny.db.prisminterface;

import java.sql.SQLException;
import java.sql.SQLFeatureNotSupportedException;
import java.util.ArrayDeque;
import java.util.Collections;
import java.util.Deque;
import java.util.IdentityHashMap;
import java.util.Set;
import java.util.regex.Pattern;
import org.apache.commons.lang3.NotImplementedException;
import org.polypheny.db.iface.AuthenticationException;
import org.polypheny.db.languages.NodeParseException;
import org.polypheny.db.runtime.SqlStateProvider;
import org.polypheny.db.util.DeadlockException;
import org.polypheny.prism.ErrorDetails;

/**
 * Classifies failures by their structured state or exception type.
 */
final class PrismErrorClassifier {

    private static final Pattern SQL_STATE = Pattern.compile( "[A-Z0-9]{5}" );


    private PrismErrorClassifier() {
    }


    static ErrorDetails details( Throwable failure, String message ) {
        ErrorDetails.Builder details = ErrorDetails.newBuilder().setMessage( message );
        String fallbackState = null;
        Set<Throwable> visited = Collections.newSetFromMap( new IdentityHashMap<>() );
        Deque<Throwable> pending = new ArrayDeque<>();
        pending.add( failure );
        while ( !pending.isEmpty() ) {
            Throwable cause = pending.removeFirst();
            if ( !visited.add( cause ) ) {
                continue;
            }
            if ( cause instanceof PIServiceException prism && isSqlState( prism.getState() ) ) {
                return prism.getPrismErrorDetails().toBuilder().setMessage( message ).build();
            }
            if ( cause instanceof SQLException sql ) {
                if ( isSqlState( sql.getSQLState() ) ) {
                    return details.setState( sql.getSQLState() ).setErrorCode( sql.getErrorCode() ).build();
                }
                SQLException next = sql.getNextException();
                if ( next != null ) {
                    pending.addLast( next );
                }
            }
            if ( cause instanceof SqlStateProvider typed && isSqlState( typed.getSqlState() ) ) {
                return details.setState( typed.getSqlState() ).build();
            }
            if ( fallbackState == null ) {
                fallbackState = stateForType( cause );
            }
            // Keep the original cause chain ahead of JDBC's additional exception links.
            Throwable nested = cause.getCause();
            if ( nested != null ) {
                pending.addFirst( nested );
            }
        }
        return details.setState( fallbackState == null ? "XX000" : fallbackState ).build();
    }


    private static boolean isSqlState( String state ) {
        return state != null && SQL_STATE.matcher( state ).matches();
    }


    private static String stateForType( Throwable failure ) {
        if ( failure instanceof NodeParseException parser && parser.isSyntaxError() ) {
            return "42601";
        }
        if ( failure instanceof AuthenticationException ) {
            return "28000";
        }
        if ( failure instanceof DeadlockException ) {
            return "40001";
        }
        if ( failure instanceof NotImplementedException || failure instanceof SQLFeatureNotSupportedException ) {
            return "0A000";
        }
        return null;
    }

}
