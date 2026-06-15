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

package org.polypheny.db.adapter.parquet.shared.io;

import java.io.File;
import java.io.IOException;
import java.io.OutputStream;
import java.nio.file.Files;
import java.nio.file.StandardOpenOption;
import org.apache.parquet.io.OutputFile;
import org.apache.parquet.io.PositionOutputStream;
import org.jetbrains.annotations.NotNull;


/**
 * A local file wrapper of Parquet {@link OutputFile} to avoid HADOOP_HOME validation access by hadoop infra.
 */
public class OutputLocalFile implements OutputFile {

    private static final long DEFAULT_BLOCK_SIZE = 64L * 1024L * 1024L;

    private final File file;


    public OutputLocalFile( File file ) {
        this.file = file;
    }


    @Override
    public PositionOutputStream create( long blockSizeHint ) throws IOException {
        return new LocalPositionOutputStream( Files.newOutputStream( file.toPath(), StandardOpenOption.CREATE_NEW, StandardOpenOption.WRITE ) );
    }


    @Override
    public PositionOutputStream createOrOverwrite( long blockSizeHint ) throws IOException {
        return new LocalPositionOutputStream( Files.newOutputStream( file.toPath(), StandardOpenOption.CREATE, StandardOpenOption.TRUNCATE_EXISTING, StandardOpenOption.WRITE ) );
    }


    @Override
    public boolean supportsBlockSize() {
        return false;
    }


    @Override
    public long defaultBlockSize() {
        return DEFAULT_BLOCK_SIZE;
    }


    @Override
    public String getPath() {
        return file.getAbsolutePath();
    }


    private static class LocalPositionOutputStream extends PositionOutputStream {

        private final OutputStream outputStream;
        private long position;


        private LocalPositionOutputStream( OutputStream outputStream ) {
            this.outputStream = outputStream;
        }


        @Override
        public long getPos() {
            return position;
        }


        @Override
        public void write( int b ) throws IOException {
            outputStream.write( b );
            position++;
        }


        @Override
        public void write( byte @NotNull [] b, int off, int len ) throws IOException {
            outputStream.write( b, off, len );
            position += len;
        }


        @Override
        public void flush() throws IOException {
            outputStream.flush();
        }


        @Override
        public void close() throws IOException {
            outputStream.close();
        }

    }

}
