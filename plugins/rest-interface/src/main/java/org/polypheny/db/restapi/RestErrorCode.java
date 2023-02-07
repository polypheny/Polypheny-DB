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

package org.polypheny.db.restapi;


public enum RestErrorCode {

    GENERIC( 0, "none", "Generic", "Something went wrong. We don't really know what though." ),

    ;

    public final int code;
    public final String subsystem;
    public final String name;
    public final String description;


    RestErrorCode( int code, String subsystem, String name, String description ) {
        this.code = code;
        this.subsystem = subsystem;
        this.name = name;
        this.description = description;
    }
}
