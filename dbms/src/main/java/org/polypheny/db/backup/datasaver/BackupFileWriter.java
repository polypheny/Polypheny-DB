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

import java.io.BufferedOutputStream;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStreamWriter;
import org.polypheny.db.catalog.exceptions.GenericRuntimeException;

/**
 * Writer that writes data to a file
 */
public class BackupFileWriter {
    File file;
    BufferedWriter out;


    /**
     * Creates a new BackupFileWriter, that writes to a file
     * @param file file to write to
     */
    public BackupFileWriter( File file) {

        this.file = file;
        try {
            this.out = new BufferedWriter( new OutputStreamWriter( new BufferedOutputStream( new FileOutputStream( file ), 32768 ) ) );
        } catch ( Exception e ) {
            throw new GenericRuntimeException( "Couldn't open file " + file.getName() + " " + e.getMessage() );
        }
    }


    /**
     * Writes a line to the file (creates a new line after the line)
     * @param string String to write
     */
    public void writeLine( String string ) {
        try {
            out.write( string );
            out.newLine();
        } catch ( Exception e ) {
            throw new GenericRuntimeException( "Couldn't write to file " + file.getName() + " " + e.getMessage() );
        }
    }


    /**
     * Writes to the file (doesn't create a new line after the line)
     * @param string String to write
     */
    public void write ( String string ) {
        try {
            out.write( string );
        } catch ( IOException e ) {
            throw new GenericRuntimeException( "Couldn't write to file " + file.getName() + " " + e.getMessage() );
        }

    }


    /**
     * Creates a new line in the file
     */
    public void newLine () {
        try {
            out.newLine();
        } catch ( IOException e ) {
            throw new GenericRuntimeException( "Couldn't write to file " + file.getName() + " " + e.getMessage() );
        }
    }


    /**
     * Flushes the writer
     */
    public void flush () {
        try {
            out.flush();
        } catch ( IOException e ) {
            throw new GenericRuntimeException( "Couldn't flush file " + file.getName() + " " + e.getMessage() );
        }
    }


    /**
     * Closes the writer
     */
    public void close () {
        try {
            out.close();
        } catch ( IOException e ) {
            throw new GenericRuntimeException( "Couldn't close file " + file.getName() + " " + e.getMessage() );
        }
    }

}
