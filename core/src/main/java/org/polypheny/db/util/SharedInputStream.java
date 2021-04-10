/*
 * Copyright 2019-2021 The Polypheny Project
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

package org.polypheny.db.util;


import com.google.common.hash.HashFunction;
import com.google.common.hash.Hashing;
import com.google.common.io.ByteStreams;
import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;
import org.polypheny.db.transaction.PolyXid;


public class SharedInputStream {

    private final PolyXid xid;
    //private final File file;
    static AtomicInteger counter = new AtomicInteger();
    private static final Map<String, List<InputStream>> inputStream = new HashMap<>();
    private static final Map<String, List<File>> files = new HashMap<>();
    private final static HashFunction SHA = Hashing.sha256();
    private final static Charset CHARSET = StandardCharsets.UTF_8;
    private final byte[] cache;
    //private final byte[] cache;

    public SharedInputStream( PolyXid xid, InputStream is ) {
        this.xid = xid;
        // todo
        // Write to byte array as long as cache.length <= MAGIC NUMBER
        // If cache.length > MAGIC NUMBER, create tmp file, write all data in cache to the file and continue
        // writing stream data to the file and set cache = null.

        try {
            this.cache = ByteStreams.toByteArray( is );
        } catch ( IOException e ) {
            throw new RuntimeException( "Could not read shared InputStream", e );
        } finally {
            try {
                is.close();
            } catch ( IOException ignored ) {
            }
        }

        /*File folder = FileSystemManager.getInstance().registerNewFolder( "tmp/" + SHA.hashString( xid.toString(), CHARSET ).toString() );
        File f = new File( folder, "sharedInputStream" + counter.incrementAndGet() );
        this.file = f;
        try {
            Files.copy( is, f.toPath() );
        } catch ( IOException e ) {
            throw new RuntimeException( "Could not write temporal file", e );
        } finally {
            try {
                is.close();
            } catch ( Exception ignored ) {
            }
        }
        if ( !files.containsKey( xid.toString() ) ) {
            files.put( xid.toString(), new ArrayList<>() );
        }
        files.get( xid.toString() ).add( f );
        */
    }


    public byte[] getData() {
        return cache;
        /*try {
            InputStream is = new FileInputStream( file );
            if ( !inputStream.containsKey( xid.toString() ) ) {
                inputStream.put( xid.toString(), new ArrayList<>() );
                inputStream.get( xid.toString() ).add( is );
            }
            return is;
        } catch ( FileNotFoundException e ) {
            throw new RuntimeException( "Temporal file does not exist" );
        }*/
        //if (file != null ) {
        // read from file
        //} else {
        // read from byte array
        //}
    }

    public static void close( PolyXid xid ) {
        /*if ( inputStream.containsKey( xid.toString() ) ) {
            inputStream.get( xid.toString() ).forEach( is -> {
                try {
                    is.close();
                } catch ( IOException ignored ) {

                }
            } );
            inputStream.get( xid.toString() ).clear();
        }
        //File f = new File( System.getProperty( "user.home" ), ".polypheny/tmp/" + SHA.hashString( xid.toString(), CHARSET ).toString() );
        FileSystemManager.getInstance().recursiveDeleteFolder( "tmp/" + SHA.hashString( xid.toString(), CHARSET ).toString() );
        */
    }

}
