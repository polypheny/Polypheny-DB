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

package org.polypheny.db.type.entity.category;

import com.google.common.io.ByteStreams;
import java.io.ByteArrayInputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import lombok.EqualsAndHashCode;
import lombok.Value;
import lombok.experimental.NonFinal;
import org.apache.calcite.avatica.util.Base64;
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


    public PolyBlob( byte @Nullable [] value, @Nullable InputStream stream ) {
        super( PolyType.FILE );
        this.value = value;
        this.stream = stream;
    }


    public static PolyBlob of( byte[] value ) {
        return new PolyBlob( value, null );
    }


    public static PolyBlob ofNullable( byte[] value ) {
        return of( value );
    }


    public static PolyBlob of( File file ) {
        try {
            return new PolyBlob( ByteStreams.toByteArray( new FileInputStream( file ) ), null );
        } catch ( IOException e ) {
            throw new GenericRuntimeException( "Could not generate FileInputStream", e );
        }
    }


    public static PolyBlob ofNullable( File file ) {
        return of( file );
    }


    public static PolyBlob of( FileInputHandle handle ) {
        return new PolyBlob( null, handle.getData() );
    }


    public static PolyBlob ofNullable( FileInputHandle handle ) {
        return of( handle );
    }


    public static PolyBlob of( InputStream stream ) {
        return new PolyBlob( null, stream );
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


    @Override
    public boolean isNull() {
        return value == null && stream == null;
    }


    @Override
    public Object toJava() {
        return value;
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


    public String as64String() {
        return Base64.encodeBytes( asByteArray() );
    }


    public boolean isHandle() {
        throw new UnsupportedOperationException( "Not yet implemented" );
    }


    public FileInputHandle getHandle() {
        throw new UnsupportedOperationException( "Not yet implemented" );
    }

}
