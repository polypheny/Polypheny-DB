/*
 * Copyright 2019-2026 The Polypheny Project
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

package org.polypheny.db.adapter.file;


import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.util.List;
import java.util.function.IntFunction;
import javax.annotation.Nullable;
import org.polypheny.db.algebra.exceptions.UniqueConstraintViolationException;
import org.polypheny.db.catalog.exceptions.GenericRuntimeException;
import org.polypheny.db.type.entity.PolyValue;


final class FilePrimaryKey {

    private FilePrimaryKey() {
    }


    static GenericRuntimeException conflict( String message, List<File> keyFolders, String existingName, IntFunction<PolyValue> candidate ) {
        // Legacy row names hash a 32-bit key hash. An occupied name alone does not prove that the keys match.
        if ( keyFolders.isEmpty() ) {
            return new GenericRuntimeException( message );
        }
        for ( int i = 0; i < keyFolders.size(); i++ ) {
            PolyValue value = candidate.apply( i );
            if ( value == null || value.isNull() || value.isBlob() || !value.equals( readValue( keyFolders.get( i ), existingName ) ) ) {
                return new GenericRuntimeException( message );
            }
        }
        return new UniqueConstraintViolationException( message );
    }


    static @Nullable PolyValue readValue( File folder, String name ) {
        try {
            return PolyValue.fromTypedJson( Files.readString( new File( folder, name ).toPath(), FileStore.CHARSET ), PolyValue.class );
        } catch ( IOException | RuntimeException e ) {
            // Incomplete or unreadable rows cannot establish a duplicate key.
            return null;
        }
    }

}
