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
 * Status of an import or export operation
 */
public class Status {
    final String context;
    long currentRow;
    float status;
    final int nTables;

    /**
     * Status constructor
     * @param context What the status is describing
     * @param nTables How many tables are being exported/imported in total
     */
    public Status( final String context, final int nTables ){
        this.context = context;
        this.currentRow = 0;
        this.status = 0f;
        this.nTables = nTables;
    }

    /**
     * Set the current status
     * @param currentRow The current row of the table that is currently being processed
     * @param totalRows The total number of rows of the table that is currently being processed
     * @param ithTable Counter to specify which table is currently being processed
     */
    public void setStatus( final long currentRow, final long totalRows, final int ithTable ) {
        this.currentRow = currentRow;
        this.status = ( (ithTable / (float)nTables) + ( currentRow / (float)totalRows ) * ( 1 / (float)nTables ) );
    }

    /**
     * Set the status to 100%
     */
    public void complete() {
        this.status = 1f;
    }
}
