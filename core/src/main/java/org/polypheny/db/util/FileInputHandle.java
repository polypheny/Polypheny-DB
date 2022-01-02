/*
 * Copyright 2019-2022 The Polypheny Project
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
import java.io.ByteArrayInputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.io.SequenceInputStream;
import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;
import org.polypheny.db.config.RuntimeConfig;
import org.polypheny.db.transaction.PolyXid;
import org.polypheny.db.transaction.Statement;


public class FileInputHandle {

    private File file;
    private byte[] cache;
    private final List<InputStream> inputStreams = new ArrayList<>();

    private static final File folder;
    private static final AtomicInteger counter = new AtomicInteger();

    private static final HashFunction SHA = Hashing.sha256();
    private static final Charset CHARSET = StandardCharsets.UTF_8;


    static {
        folder = PolyphenyHomeDirManager.getInstance().registerNewFolder( "tmp/" + "fileInput" );
    }


    public FileInputHandle( Statement statement, InputStream is ) {
        statement.registerFileInputHandle( this );
        PolyXid xid = statement.getTransaction().getXid();
        String fileName = SHA.hashString( xid.toString(), CHARSET ).toString() + "-" + counter.incrementAndGet();
        int cacheSize = RuntimeConfig.FILE_HANDLE_CACHE_SIZE.getInteger();

        // Write to byte array as long as cache.length <= MAGIC NUMBER
        // If cache.length > MAGIC NUMBER, create tmp file, write all data in cache to the file and continue
        // writing stream data to the file
        try {
            if ( cacheSize > 0 ) {
                byte[] bytes = new byte[cacheSize];
                int remaining = cacheSize;
                while ( remaining > 0 ) {
                    int off = cacheSize - remaining;
                    int read = is.read( bytes, off, 1 );
                    if ( read == -1 ) {
                        // end of stream before reading expectedSize bytes
                        // just return the bytes read so far
                        cache = Arrays.copyOf( bytes, off );
                        break;
                    }
                    remaining -= read;
                }

                // bytes is now full
                if ( cache == null ) {
                    this.file = new File( folder, fileName );
                    try ( InputStream byteCacheStream = new ByteArrayInputStream( bytes );
                            InputStream chainedInputStream = new SequenceInputStream( byteCacheStream, is ) ) {
                        Files.copy( chainedInputStream, file.toPath() );
                    }
                }
            } else {
                this.file = new File( folder, fileName );
                Files.copy( is, file.toPath() );
            }
        } catch ( IOException e ) {
            throw new RuntimeException( "Exception while creating FileInputHandle", e );
        } finally {
            try {
                is.close();
            } catch ( IOException ignored ) {
            }
        }
    }


    public InputStream getData() {
        try {
            InputStream is;
            if ( file != null ) {
                is = new FileInputStream( file );
            } else {
                is = new ByteArrayInputStream( cache );
            }
            inputStreams.add( is );
            return is;
        } catch ( FileNotFoundException e ) {
            throw new RuntimeException( "Temporal file does not exist" );
        }
    }


    public Path materializeAsFile( Path path ) throws IOException {
        if ( file != null ) {
            // TODO: check if same filesystem
            // Create hardlink
            return Files.createLink( path, file.toPath() );
        } else {
            try ( InputStream cacheStream = new ByteArrayInputStream( cache ) ) {
                Files.copy( cacheStream, path );
                return path;
            }
        }
    }


    public void close() {
        for ( InputStream inputStream : inputStreams ) {
            try {
                inputStream.close();
            } catch ( IOException e ) {
                // ignore
            }
        }
        if ( file != null ) {
            file.delete();
        }
        cache = null;
    }


    public ContentInfo getContentType( final ContentInfoUtil util ) throws IOException {
        if ( file != null ) {
            return util.findMatch( file );
        } else {
            return util.findMatch( cache );
        }
    }

}
