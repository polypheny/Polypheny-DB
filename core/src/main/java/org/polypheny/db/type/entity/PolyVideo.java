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

import org.apache.commons.lang3.ArrayUtils;
import org.jetbrains.annotations.Nullable;
import org.polypheny.db.type.PolyType;
import org.polypheny.db.type.entity.category.PolyBlob;

public class PolyVideo extends PolyBlob {


    public final Object value;


    public PolyVideo( Object value ) {
        super( PolyType.VIDEO );
        this.value = value;
    }


    public static PolyVideo of( Object value ) {
        return new PolyVideo( value );
    }


    public static PolyVideo ofNullable( byte[] value ) {
        return of( value );
    }


    public static PolyVideo ofNullable( @Nullable Byte[] value ) {
        return value == null ? new PolyVideo( null ) : new PolyVideo( ArrayUtils.toPrimitive( value ) );
    }

}
