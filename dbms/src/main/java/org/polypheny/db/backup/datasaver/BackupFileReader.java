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

package org.polypheny.db.backup.datasaver;

import java.io.BufferedInputStream;
import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.InputStreamReader;
import org.polypheny.db.catalog.exceptions.GenericRuntimeException;

/**
 * Reader that streams data from a file
 */
public class BackupFileReader {

    File file;
    BufferedReader in;


    /**
     * Creates a new BackupFileReader, that reads from a file
     * @param file file to read from
     */
    public BackupFileReader( File file ) {
        this.file = file;
        try {
            this.in = new BufferedReader( new InputStreamReader( new BufferedInputStream( new FileInputStream( file ), 32768 ) ) );
        } catch ( Exception e ) {
            throw new GenericRuntimeException( "Couldn't open file " + file.getName() + " " + e.getMessage() );
        }
    }


    /**
     * Reads a line from the file
     * @return returns the read line as a String
     */
    public String readLine() {
        try {
            return in.readLine();
        } catch ( Exception e ) {
            throw new GenericRuntimeException( "Couldn't read from file " + file.getName() + " " + e.getMessage() );
        }
    }


    /**
     * Closes the reader
     */
    public void close() {
        try {
            in.close();
        } catch ( Exception e ) {
            throw new GenericRuntimeException( "Couldn't close file " + file.getName() + " " + e.getMessage() );
        }
    }


}
