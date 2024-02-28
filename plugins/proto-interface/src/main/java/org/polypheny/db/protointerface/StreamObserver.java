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

import java.io.IOException;
import java.io.OutputStream;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.concurrent.atomic.AtomicBoolean;
import org.polypheny.db.catalog.exceptions.GenericRuntimeException;
import org.polypheny.db.protointerface.proto.Request;
import org.polypheny.db.protointerface.proto.Response;

public class StreamObserver<T> {

    private final Request request;

    private final OutputStream out;

    private final String type;

    private final AtomicBoolean done;


    StreamObserver( Request request, OutputStream out, String type, AtomicBoolean done ) {
        this.request = request;
        this.out = out;
        this.type = type;
        this.done = done;
    }


    public void onNext( T response, boolean last ) {
        if ( done.get() ) {
            throw new GenericRuntimeException( "No more messages allowed" );
        }
        Response r = Response.newBuilder()
                .setId( request.getId() )
                .setLast( last )
                .setField( Response.getDescriptor().findFieldByName( type ), response )
                .build();
        byte[] b = r.toByteArray();
        ByteBuffer bb = ByteBuffer.allocate( 8 );
        bb.order( ByteOrder.LITTLE_ENDIAN );
        bb.putLong( b.length );
        try {
            out.write( bb.array() );
            out.write( b );
            done.set( last );
        } catch ( IOException e ) {
            throw new GenericRuntimeException( e );
        }
    }


    public void onNext( T response ) {
        onNext( response, true );
    }


    public void onCompleted() {
    }

}
