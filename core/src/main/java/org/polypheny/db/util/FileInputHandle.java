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
import com.j256.simplemagic.ContentInfo;
import com.j256.simplemagic.ContentInfoUtil;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;
import org.polypheny.db.transaction.PolyXid;
import org.polypheny.db.transaction.Statement;


public class FileInputHandle {

    private final File file;
    private final List<InputStream> inputStreams = new ArrayList<>();

    private static final File folder;
    private static final AtomicInteger counter = new AtomicInteger();

    private static final HashFunction SHA = Hashing.sha256();
    private static final Charset CHARSET = StandardCharsets.UTF_8;
    //private byte[] cache;


    static {
        folder = FileSystemManager.getInstance().registerNewFolder( "tmp/" + "fileInput" );
    }


    public FileInputHandle( Statement statement, InputStream is ) {
        statement.registerFileInputHandle( this );
        PolyXid xid = statement.getTransaction().getXid();
        // todo
        // Write to byte array as long as cache.length <= MAGIC NUMBER
        // If cache.length > MAGIC NUMBER, create tmp file, write all data in cache to the file and continue
        // writing stream data to the file and set cache = null.

        /*try {
            this.cache = ByteStreams.toByteArray( is );
        } catch ( IOException e ) {
            throw new RuntimeException( "Could not read shared InputStream", e );
        } finally {
            try {
                is.close();
            } catch ( IOException ignored ) {
            }
        }*/

        File f = new File( folder, SHA.hashString( xid.toString(), CHARSET ).toString() + "-" + counter.incrementAndGet() );
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
    }


    public InputStream getData() {
        //return cache;
        try {
            InputStream is = new FileInputStream( file );
            inputStreams.add( is );
            return is;
        } catch ( FileNotFoundException e ) {
            throw new RuntimeException( "Temporal file does not exist" );
        }
        //if (file != null ) {
        // read from file
        //} else {
        // read from byte array
        //}
    }


    public Path materializeAsFile( Path path ) throws IOException {
        //todo check if same filesystem
        return Files.createLink( path, file.toPath() );
    }


    public void close() {
        for ( InputStream inputStream : inputStreams ) {
            try {
                inputStream.close();
            } catch ( IOException e ) {
                // ignore
            }
        }
        file.delete();
    }


    public ContentInfo getContentType( final ContentInfoUtil util ) throws IOException {
        return util.findMatch( file );
    }

}
