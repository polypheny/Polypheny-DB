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

package org.polypheny.db.catalog.exceptions;

import lombok.Getter;

public class UnknownTriggerException extends CatalogException {

    @Getter
    private final String triggerName;


    public UnknownTriggerException(String databaseName, String schemaName, String triggerName ) {
        super( "There is no trigger with name '" + triggerName + "' in schema '" + schemaName + "' of database '" + databaseName + "'" );
        this.triggerName = triggerName;
    }


    public UnknownTriggerException(long databaseId, long schemaId, String triggerName) {
        super( "There is no trigger with name '" + triggerName + "' in schema '" + schemaId + "' of database '" + databaseId + "'" );
        this.triggerName = triggerName;
    }


    public UnknownTriggerException(long schemaId, String triggerName) {
        super( "There is no trigger with name '" + triggerName + "' in schema '" + schemaId + "'" );
        this.triggerName = triggerName;
    }

}
