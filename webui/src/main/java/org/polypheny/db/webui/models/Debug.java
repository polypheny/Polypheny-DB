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

package org.polypheny.db.webui.models;


/**
 * Infos about a query, e.g. number of affected rows
 */
public class Debug {

    private int affectedRows;
    private String generatedQuery;


    public Debug setAffectedRows( final int affectedRows ) {
        this.affectedRows = affectedRows;
        return this;
    }


    public Debug setGeneratedQuery( final String query ) {
        this.generatedQuery = query;
        return this;
    }


    public Debug update( final Debug debug ) {
        if ( debug.affectedRows != 0 ) {
            this.affectedRows = debug.affectedRows;
        }
        if ( debug.generatedQuery != null ) {
            this.generatedQuery = debug.generatedQuery;
        }
        return this;
    }
}
