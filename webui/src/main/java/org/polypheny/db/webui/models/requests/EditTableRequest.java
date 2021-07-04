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

package org.polypheny.db.webui.models.requests;


import org.polypheny.db.webui.models.DbColumn;


/**
 * Model for a request to edit or create a Table used for request where you want to truncate/drop a table
 * and when you want to create a new table
 */
public class EditTableRequest {

    public String schema;
    public String table;
    public String action; // truncate / drop
    public DbColumn[] columns;
    public String store;
    public String tableType;

}
