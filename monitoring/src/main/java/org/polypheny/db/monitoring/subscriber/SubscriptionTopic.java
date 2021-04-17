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

package org.polypheny.db.monitoring.subscriber;


import org.polypheny.db.monitoring.exceptions.UnknownSubscriptionTopicRuntimeException;


public enum SubscriptionTopic {
    ALL( 0 ),
    STORE( 1 ),
    TABLE( 2 );

    private final int id;


    SubscriptionTopic( int id ) {
        this.id = id;
    }


    public int getId() {
        return id;
    }


    public static SubscriptionTopic getById( final int id ) {
        for ( SubscriptionTopic t : values() ) {
            if ( t.id == id ) {
                return t;
            }
        }
        throw new UnknownSubscriptionTopicRuntimeException( id );
    }


    public static SubscriptionTopic getByName( final String name ) throws UnknownSubscriptionTopicRuntimeException {
        for ( SubscriptionTopic t : values() ) {
            if ( t.name().equalsIgnoreCase( name ) ) {
                return t;
            }
        }
        throw new UnknownSubscriptionTopicRuntimeException( name );
    }

}