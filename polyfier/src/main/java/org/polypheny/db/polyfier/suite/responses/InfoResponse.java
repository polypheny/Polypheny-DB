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

package org.polypheny.db.polyfier.suite.responses;

public class InfoResponse extends SuiteResponse {

    private InfoResponse( String message, String code ) {
        this.message = message;
        this.code = code;
    }

    public static InfoResponse invalidRequest( String message ) {
        return new InfoResponse( "Invalid Request: " + message, ERROR_CODE );
    }

    public static InfoResponse ok() {
        return new InfoResponse( "OK", OK_CODE );
    }

    public static InfoResponse ok( String message ) {
        return new InfoResponse( "OK: " + message, OK_CODE );
    }

}
