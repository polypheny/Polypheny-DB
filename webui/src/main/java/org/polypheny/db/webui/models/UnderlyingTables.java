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

package org.polypheny.db.webui.models;

import java.util.List;
import java.util.Map;

public class UnderlyingTables {

    Throwable exception;
    Map<String, List<String>> underlyingTable;


    public UnderlyingTables( Map<String, List<String>> underlyingTable ) {
        this.underlyingTable = underlyingTable;
    }


    public UnderlyingTables( final Throwable exception ) {
        this.exception = exception;
    }

}
