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
 */

package org.polypheny.db.processing.replication;


import org.polypheny.db.nodes.Node;
import org.polypheny.db.processing.replication.exceptions.UnknownIsolationLevelException;
import org.polypheny.db.replication.IsolationLevel;


public abstract class IsolationExtractor {

    protected IsolationLevel isolationLevel;
    protected Node isolationNode;


    protected IsolationExtractor( Node isolationNode ) {
        this.isolationNode = isolationNode;
    }


    /**
     * Extracts IsolationLevel from any query, if it was specified.
     * Is used to enrich a transaction to have access to these specifications later on.
     *
     * @return General IsolationLevel unrelated to a specific query language.
     */
    public abstract IsolationLevel extractIsolationLevel() throws UnknownIsolationLevelException;


    public IsolationLevel getIsolationLevel() {

        if ( isolationLevel == null ) {
            throw new RuntimeException( "IsolationLevel has not been extracted yet." );
        }

        return isolationLevel;
    }


    protected void setIsolationLevel( IsolationLevel isolationLevel ) {
        this.isolationLevel = isolationLevel;
    }

}
