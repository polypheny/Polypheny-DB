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

package org.polypheny.db.type.entity;

import com.datastax.oss.driver.shaded.guava.common.io.ByteStreams;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import org.apache.commons.lang3.ArrayUtils;
import org.jetbrains.annotations.Nullable;
import org.polypheny.db.catalog.exceptions.GenericRuntimeException;
import org.polypheny.db.type.PolyType;
import org.polypheny.db.type.entity.category.PolyBlob;
import org.polypheny.db.util.FileInputHandle;

public class PolyVideo extends PolyBlob {


    private PolyVideo( byte @Nullable [] value, @Nullable InputStream stream ) {
        super( PolyType.VIDEO, value, stream );
    }


    public static PolyVideo of( byte[] value ) {
        return new PolyVideo( value, null );
    }


    public static PolyVideo of( File file ) {
        try {
            return new PolyVideo( ByteStreams.toByteArray( new FileInputStream( file ) ), null );
        } catch ( IOException e ) {
            throw new GenericRuntimeException( "Could not generate FileInputStream", e );
        }
    }


    public static PolyVideo of( FileInputHandle handle ) {
        return new PolyVideo( null, handle.getData() );
    }


    public static PolyVideo of( InputStream stream ) {
        return new PolyVideo( null, stream );
    }


    public static PolyVideo ofNullable( byte[] value ) {
        return of( value );
    }


    public static PolyVideo ofNullable( InputStream stream ) {
        return of( stream );
    }


    public static PolyVideo ofNullable( @Nullable Byte[] value ) {
        return value == null ? new PolyVideo( null, null ) : new PolyVideo( ArrayUtils.toPrimitive( value ), null );
    }


    @Override
    public String toJson() {
        return value == null ? null : super.toJson();
    }

}
