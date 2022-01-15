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

package org.polypheny.db.adapter;

import java.util.Collections;
import java.util.List;
import lombok.Getter;

public enum DeployMode {

    REMOTE( "remote" ),
    DOCKER( "docker" ),
    EMBEDDED( "embedded" );

    @Getter
    private final String name;


    DeployMode( String name ) {
        this.name = name;
    }


    public static DeployMode fromString( String mode ) {
        if ( mode.equals( "remote" ) ) {
            return REMOTE;
        } else if ( mode.equals( "docker" ) ) {
            return DOCKER;
        } else {
            return EMBEDDED;
        }
    }


    public enum DeploySetting {
        REMOTE( DeployMode.REMOTE ),
        DOCKER( DeployMode.DOCKER ),
        EMBEDDED( DeployMode.EMBEDDED ),
        DEFAULT;

        private final DeployMode mode;
        @Getter
        private boolean usedByAll = false;


        DeploySetting( DeployMode mode ) {
            this.mode = mode;
        }


        DeploySetting() {
            usedByAll = true;
            mode = DeployMode.EMBEDDED;
        }


        /**
         * DeploySettings can wrap multiple underlying DeployModes
         * this method returns them
         *
         * @param availableModes All available modes, to which this setting could belong
         * @return The modes for which this setting is available
         */
        List<DeployMode> getModes( List<DeployMode> availableModes ) {
            if ( usedByAll ) {
                return availableModes;
            } else {
                return Collections.singletonList( mode );
            }
        }

    }
}