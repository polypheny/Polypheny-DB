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


import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrowsExactly;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Arrays;
import java.util.List;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.polypheny.db.algebra.exceptions.UniqueConstraintViolationException;
import org.polypheny.db.catalog.exceptions.GenericRuntimeException;
import org.polypheny.db.type.entity.PolyValue;
import org.polypheny.db.type.entity.numerical.PolyInteger;
import org.polypheny.db.util.PolyphenyHomeDirManager;
import org.polypheny.db.util.RunMode;


class FilePrimaryKeyTest {

    private static final String STAGING_MESSAGE = "Primary key conflict! You are trying to insert a row with a primary key that already exists.";
    private static final String COMMIT_MESSAGE = "Could not commit: a row with the same primary key already exists";

    @TempDir
    Path root;

    private List<PolyValue> existingKey;
    private List<PolyValue> collidingKey;
    private String committedName;
    private String stagedName;
    private List<File> keyFolders;


    @BeforeAll
    static void initializePolypheny() {
        if ( PolyphenyHomeDirManager.getMode() == null ) {
            PolyphenyHomeDirManager.setModeAndGetInstance( RunMode.TEST );
        }
    }


    @BeforeEach
    void prepareKeyFiles() throws IOException {
        existingKey = List.of( PolyInteger.of( 1 ), PolyInteger.of( 42 ) );
        collidingKey = List.of( PolyInteger.of( 2 ), PolyInteger.of( 32 ) );
        committedName = legacyRowName( existingKey );
        stagedName = "_ins_" + FileStore.SHA.hashString( "transaction", FileStore.CHARSET ) + "_" + committedName;
        keyFolders = List.of( Files.createDirectory( root.resolve( "col1_3" ) ).toFile(), Files.createDirectory( root.resolve( "col2_3" ) ).toFile() );
    }


    @Test
    void comparesStoredKeyWhenStagingAnotherRowWithTheSameName() throws IOException {
        assertEquals( committedName, legacyRowName( collidingKey ) );
        writeKey( stagedName, existingKey );
        Integer[] keyColumns = { 0, 1 };

        GenericRuntimeException collision = assertThrowsExactly( GenericRuntimeException.class,
                () -> FileModifier.requireUnusedStagedTarget( keyFolders, keyColumns, stagedName, collidingKey ) );
        assertEquals( STAGING_MESSAGE, collision.getMessage() );

        UniqueConstraintViolationException duplicate = assertThrowsExactly( UniqueConstraintViolationException.class,
                () -> FileModifier.requireUnusedStagedTarget( keyFolders, keyColumns, stagedName, existingKey ) );
        assertEquals( "23505", duplicate.getSqlState() );
        assertEquals( STAGING_MESSAGE, duplicate.getMessage() );
        assertEquals( existingKey.get( 1 ), FilePrimaryKey.readValue( keyFolders.get( 1 ), stagedName ) );
    }


    @Test
    void collisionAtCommitDoesNotReportDuplicate() throws IOException {
        assertEquals( committedName, legacyRowName( collidingKey ) );
        writeKey( committedName, existingKey );
        writeKey( stagedName, collidingKey );

        GenericRuntimeException collision = assertThrowsExactly( GenericRuntimeException.class, this::checkCommit );
        assertEquals( COMMIT_MESSAGE, collision.getMessage() );
        assertEquals( existingKey.get( 1 ), FilePrimaryKey.readValue( keyFolders.get( 1 ), committedName ) );
        assertEquals( collidingKey.get( 1 ), FilePrimaryKey.readValue( keyFolders.get( 1 ), stagedName ) );
    }


    @Test
    void duplicateAtCommitReportsSqlState() throws IOException {
        writeKey( committedName, existingKey );
        writeKey( stagedName, existingKey );

        UniqueConstraintViolationException duplicate = assertThrowsExactly( UniqueConstraintViolationException.class, this::checkCommit );
        assertEquals( "23505", duplicate.getSqlState() );
        assertEquals( COMMIT_MESSAGE, duplicate.getMessage() );
    }


    @Test
    void unverifiableKeyDoesNotEstablishDuplicateAtCommit() throws IOException {
        writeKey( committedName, existingKey );
        FileModifier.write( new File( keyFolders.get( 0 ), stagedName ), existingKey.get( 0 ) );

        GenericRuntimeException conflict = assertThrowsExactly( GenericRuntimeException.class, this::checkCommit );
        assertEquals( COMMIT_MESSAGE, conflict.getMessage() );

        Files.writeString( keyFolders.get( 1 ).toPath().resolve( stagedName ), "broken", FileStore.CHARSET );
        conflict = assertThrowsExactly( GenericRuntimeException.class, this::checkCommit );
        assertEquals( COMMIT_MESSAGE, conflict.getMessage() );
    }


    private static String legacyRowName( List<PolyValue> key ) {
        int hash = Arrays.hashCode( key.stream().map( PolyValue::toJson ).toArray() );
        return FileStore.SHA.hashString( String.valueOf( hash ), FileStore.CHARSET ).toString();
    }


    private void writeKey( String name, List<PolyValue> key ) throws IOException {
        for ( int i = 0; i < key.size(); i++ ) {
            FileModifier.write( new File( keyFolders.get( i ), name ), key.get( i ) );
        }
    }


    private void checkCommit() {
        File folder = keyFolders.get( 0 );
        FileStore.requireUnusedCommitTarget( new File( folder, stagedName ), new File( folder, committedName ), ignored -> keyFolders );
    }

}
