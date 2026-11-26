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


import static java.util.concurrent.TimeUnit.SECONDS;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrowsExactly;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.polypheny.db.catalog.exceptions.GenericRuntimeException;


class FileStoreTransactionTest {

    @TempDir
    Path root;


    @Test
    void continuesScanningStagedFilesAfterAnotherTransactionDropsAColumn() throws Exception {
        Path dropped = Files.createDirectory( root.resolve( "col1_1" ) );
        Path retained = Files.createDirectory( root.resolve( "col2_2" ) );
        String insertPrefix = "_ins_transaction_";
        String deletePrefix = "_del_transaction_";
        File inserted = Files.createFile( retained.resolve( insertPrefix + "first" ) ).toFile();
        File deleted = Files.createFile( retained.resolve( deletePrefix + "second" ) ).toFile();
        Files.createFile( retained.resolve( "_ins_other_third" ) );

        File[] folders = root.toFile().listFiles( File::isDirectory );
        assertEquals( 2, folders.length );
        assertTrue( CompletableFuture.supplyAsync( dropped.toFile()::delete ).get( 5, SECONDS ) );

        assertEquals( List.of( inserted ), stagedInFolders( folders, insertPrefix ) );
        assertEquals( List.of( deleted ), stagedInFolders( folders, deletePrefix ) );
    }


    @Test
    void reportsListingFailuresForExistingPaths() throws IOException {
        File column = Files.createFile( root.resolve( "col1_1" ) ).toFile();
        GenericRuntimeException error = assertThrowsExactly( GenericRuntimeException.class,
                () -> FileStore.listStagedFiles( column, "_ins_transaction_" ) );
        assertEquals( "Could not read staged files in " + column, error.getMessage() );
    }


    private static List<File> stagedInFolders( File[] folders, String prefix ) {
        return Arrays.stream( folders ).flatMap( folder -> Arrays.stream( FileStore.listStagedFiles( folder, prefix ) ) ).toList();
    }

}
