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

package org.polypheny.db.transaction.locking;


import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.util.concurrent.atomic.AtomicLong;

public class EntityIdentifierGenerator {

    public static final EntityIdentifierGenerator INSTANCE = new EntityIdentifierGenerator();
    private static final String COUNTER_LOG = "entry_identifier_counter.dat";

    private final AtomicLong counter;


    private EntityIdentifierGenerator() {
        this.counter = new AtomicLong( loadCounterValue() );
    }


    public long getEntryIdentifier() {
        return counter.incrementAndGet();
    }


    public void shutdown() {
        saveCounterValue( counter.get() );
    }


    private long loadCounterValue() {
        File file = new File( COUNTER_LOG );
        if ( !file.exists() ) {
            return 0;
        }

        try ( DataInputStream dis = new DataInputStream( new FileInputStream( file ) ) ) {
            return dis.readLong();
        } catch ( IOException e ) {
            System.err.println( "Error loading counter value, starting from 0: " + e.getMessage() );
            return 0;
        }
    }


    private void saveCounterValue( long value ) {
        try ( DataOutputStream dos = new DataOutputStream( new FileOutputStream( COUNTER_LOG ) ) ) {
            dos.writeLong( value );
        } catch ( IOException e ) {
            System.err.println( "Error saving counter value: " + e.getMessage() );
        }
    }

}
