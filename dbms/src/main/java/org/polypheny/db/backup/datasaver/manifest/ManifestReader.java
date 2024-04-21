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

package org.polypheny.db.backup.datasaver.manifest;

import com.google.gson.Gson;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.io.Reader;
import org.polypheny.db.catalog.exceptions.GenericRuntimeException;

/**
 * Transforms the manifest file from the backup into a BackupManifest object
 */
public class ManifestReader {

    /**
     * Reads a manifest file and returns the BackupManifest object
     * @param manifestFilePath path to the manifest file
     * @return BackupManifest object
     */
    public BackupManifest readManifest( String manifestFilePath ) {
        try (
                Reader reader = new FileReader( manifestFilePath );
            )
        {
            Gson gson = new Gson();
            return gson.fromJson( reader, BackupManifest.class );

        } catch ( FileNotFoundException e ) {
            throw new GenericRuntimeException( "Manifest was not found" + e.getMessage());
        } catch ( IOException e ) {
            throw new GenericRuntimeException( "Couldn't read manifest" + e.getMessage() );
        }
    }

}
