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


import java.util.HashMap;
import java.util.UUID;


public class InformationKeyValue extends Information {

    private final HashMap<String, String> keyValuePairs = new HashMap<>();


    /**
     * Constructor
     *
     * @param group The InformationGroup to which this information belongs
     */
    public InformationKeyValue( InformationGroup group ) {
        super( UUID.randomUUID().toString(), group.getId() );
    }


    /**
     * Constructor
     *
     * @param id Unique id for this Information object
     * @param group The InformationGroup to which this information belongs
     */
    InformationKeyValue( String id, InformationGroup group ) {
        super( id, group.getId() );
    }


    public InformationKeyValue putPair( final String key, final String value ) {
        this.keyValuePairs.put( key, value );
        this.notifyManager();
        return this;
    }


    public InformationKeyValue removePair( final String key ) {
        this.keyValuePairs.remove( key );
        this.notifyManager();
        return this;
    }


    public String getValue( final String key ) {
        return this.keyValuePairs.get( key );
    }

}
