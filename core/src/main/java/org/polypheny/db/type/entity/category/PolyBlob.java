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

package org.polypheny.db.type.entity.category;

import com.datastax.oss.driver.shaded.guava.common.io.ByteStreams;
import java.io.ByteArrayInputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import lombok.EqualsAndHashCode;
import lombok.Value;
import lombok.experimental.NonFinal;
import org.apache.calcite.linq4j.tree.Expression;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;
import org.polypheny.db.catalog.exceptions.GenericRuntimeException;
import org.polypheny.db.type.PolySerializable;
import org.polypheny.db.type.PolyType;
import org.polypheny.db.type.entity.PolyValue;
import org.polypheny.db.util.FileInputHandle;

@EqualsAndHashCode(callSuper = true)
@Value
@NonFinal
public class PolyBlob extends PolyValue {


    public byte[] value;
    public InputStream stream;


    public PolyBlob( PolyType type, byte @Nullable [] value, @Nullable InputStream stream ) {
        super( type );
        this.value = value;
        this.stream = stream;
    }


    public static PolyBlob of( PolyType type, byte[] value ) {
        return new PolyBlob( type, value, null );
    }


    public static PolyBlob ofNullable( PolyType type, byte[] value ) {
        return of( type, value );
    }


    public static PolyBlob ofNullable( byte[] value ) {
        return of( null, value );
    }


    public static PolyBlob of( PolyType type, File file ) {
        try {
            return new PolyBlob( type, ByteStreams.toByteArray( new FileInputStream( file ) ), null );
        } catch ( IOException e ) {
            throw new GenericRuntimeException( "Could not generate FileInputStream", e );
        }
    }


    public static PolyBlob ofNullable( PolyType type, File file ) {
        return of( type, file );
    }


    public static PolyBlob of( PolyType type, FileInputHandle handle ) {
        return new PolyBlob( type, null, handle.getData() );
    }


    public static PolyBlob ofNullable( PolyType type, FileInputHandle handle ) {
        return of( type, handle );
    }


    public static PolyBlob of( PolyType type, InputStream stream ) {
        return new PolyBlob( type, null, stream );
    }


    @Override
    public @Nullable Long deriveByteSize() {
        return null;
    }


    @Override
    public int compareTo( @NotNull PolyValue o ) {
        return 0;
    }


    @Override
    public Expression asExpression() {
        return null;
    }


    @Override
    public PolySerializable copy() {
        return null;
    }


    public InputStream asBinaryStream() {
        if ( stream != null ) {
            return stream;
        }
        if ( value == null ) {
            return null;
        }
        return new ByteArrayInputStream( value );
    }


    public byte[] asByteArray() {
        if ( value != null ) {
            return value;
        }
        if ( stream == null ) {
            return null;
        }

        try {
            return ByteStreams.toByteArray( stream );
        } catch ( IOException e ) {
            throw new GenericRuntimeException( e );
        }
    }

}
