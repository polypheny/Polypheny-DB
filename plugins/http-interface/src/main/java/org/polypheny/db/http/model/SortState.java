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

package org.polypheny.db.http.model;


/**
 * Defines how a column is sorted.
 * Required for Gson.
 */
public class SortState {

    /**
     * How the column is supposed to be sorted (ASC or DESC)
     */
    public SortDirection direction;


    /**
     * If true, the column will be sorted
     */
    public boolean sorting;


    /**
     * Column to be sorted
     * needed for the PlanBuilder
     */
    public String column;


    public SortState() {
        this.direction = SortDirection.DESC;
        this.sorting = false;
    }


    public SortState( final SortDirection direction ) {
        this.direction = direction;
        this.sorting = true;
    }

}
