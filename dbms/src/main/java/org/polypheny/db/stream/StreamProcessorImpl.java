/*
 * Copyright 2019-2023 The Polypheny Project
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

package org.polypheny.db.stream;

import static org.reflections.Reflections.log;

import java.util.List;
import org.polypheny.db.PolyImplementation;
import org.polypheny.db.algebra.AlgRoot;
import org.polypheny.db.transaction.Statement;
import org.polypheny.db.transaction.TransactionException;

public class StreamProcessorImpl implements StreamProcessor {

    String stream;


    public StreamProcessorImpl( String stream ) {
        this.stream = stream;
    }


    @Override
    public String getStream() {
        return stream;
    }


    public boolean isNumber( String value ) {
        try {
            Double.parseDouble( value );
        } catch ( NumberFormatException e ) {
            return false;
        }
        return true;
    }


    public boolean isBoolean( String value ) {
        return value.equals( "true" ) || value.equals( "false" );
    }


    protected List<List<Object>> executeAndTransformPolyAlg( AlgRoot algRoot, Statement statement ) {

        try {
            PolyImplementation result = statement.getQueryProcessor().prepareQuery( algRoot, false );
            log.debug( "AlgRoot was prepared." );
            List<List<Object>> rows = result.getRows( statement, -1 );
            statement.getTransaction().commit();
            return rows;
        } catch ( Throwable e ) {
            log.error( "Error during execution of stream processor query", e );
            try {
                statement.getTransaction().rollback();
            } catch ( TransactionException transactionException ) {
                log.error( "Could not rollback", e );
            }
            return null;
        }
    }

}
