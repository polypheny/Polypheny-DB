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

package org.polypheny.db.information;


import java.util.HashMap;
import java.util.UUID;
import lombok.Getter;


/**
 * Is displayed in the UI as a button
 */
public class InformationAction extends Information {

    @SuppressWarnings({ "FieldCanBeLocal", "unused" })
    private String label;
    private transient Action action;
    @Getter
    private HashMap<String, String> parameters = new HashMap<>();


    /**
     * Constructor
     *
     * @param group The InformationGroup to which this information belongs
     */
    public InformationAction( final InformationGroup group, final String buttonLabel, final Action action ) {
        super( UUID.randomUUID().toString(), group.getId() );
        this.action = action;
        this.label = buttonLabel;
    }


    public InformationAction withParameters( final String... actions ) {
        for ( String a : actions ) {
            this.parameters.put( a, "" );
        }
        return this;
    }


    public String executeAction( final HashMap<String, String> parameters ) {
        return this.action.run( parameters );
    }


    /**
     * Action of an InformationAction that should be executed
     */
    public interface Action {

        String run( final HashMap<String, String> parameters );
    }

}
