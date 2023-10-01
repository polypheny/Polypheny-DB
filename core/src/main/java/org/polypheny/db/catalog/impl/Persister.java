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

package org.polypheny.db.catalog.impl;

import com.drew.lang.Charsets;
import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import org.polypheny.db.catalog.exceptions.GenericRuntimeException;
import org.polypheny.db.util.PolyphenyHomeDirManager;

public class Persister {

    ExecutorService service = Executors.newSingleThreadExecutor();


    private final File backup;


    public Persister() {
        this.backup = initBackupFile();
    }


    private static File initBackupFile() {
        if ( !PolyphenyHomeDirManager.getInstance().checkIfExists( "catalog" ) ) {
            PolyphenyHomeDirManager.getInstance().registerNewFolder( "catalog" );
        }
        File folder = PolyphenyHomeDirManager.getInstance().getFileIfExists( "catalog" );
        if ( !folder.isDirectory() ) {
            throw new GenericRuntimeException( "There is an error with the catalog folder in the .polypheny folder." );
        }
        return PolyphenyHomeDirManager.getInstance().registerNewFile( "catalog/catalog.poly" );
    }


    public synchronized void write( String data ) {
        service.execute( () -> {
            try {
                FileWriter writer = new FileWriter( backup, Charsets.ISO_8859_1 );
                writer.write( data );
                writer.flush();
                writer.close();
            } catch ( IOException e ) {
                throw new GenericRuntimeException( e );
            }
        } );
    }


    public synchronized String read() {
        //String data;
        StringBuilder data = new StringBuilder();
        try {
            BufferedReader reader = new BufferedReader( new FileReader( backup, Charsets.ISO_8859_1 ) );

            int c;
            while ( ((c = reader.read()) != -1) ) {
                data.append( (char) c );
            }
            reader.close();
        } catch ( IOException e ) {
            throw new GenericRuntimeException( e );
        }
        return data.toString();
    }

}
