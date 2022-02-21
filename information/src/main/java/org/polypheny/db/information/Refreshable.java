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

package org.polypheny.db.information;


public abstract class Refreshable {

    /**
     * Refresh function to load on demand
     */
    private transient RefreshFunction refreshFunction;

    /**
     * Indicates if the refreshFunction exists, needed for the UI
     */
    private boolean refreshable = false;


    /**
     * Set a refresh function, to load the InformationPage on demand
     */
    public void setRefreshFunction( final RefreshFunction refreshFunction ) {
        this.refreshFunction = refreshFunction;
        this.refreshable = true;
    }


    public void refresh() {
        if ( this.refreshable && this.refreshFunction != null ) {
            this.refreshFunction.refresh();
        }
    }


    public interface RefreshFunction {
        void refresh();

    }

}
