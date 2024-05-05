/*
 * Copyright 2019-2024 The Polypheny Project
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

package org.polypheny.db.protointerface;

import org.polypheny.prism.Request;
import org.polypheny.prism.Response;

public class ResponseMaker<T> {

    private final Request req;
    private final String field;


    ResponseMaker( Request req, String field ) {
        this.req = req;
        this.field = field;
    }


    Response makeResponse( T msg ) {
        return makeResponse( msg, true );
    }


    Response makeResponse( T msg, boolean isLast ) {
        return Response.newBuilder()
                .setId( req.getId() )
                .setLast( isLast )
                .setField( Response.getDescriptor().findFieldByName( field ), msg )
                .build();
    }

}
