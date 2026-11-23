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

package org.polypheny.db.webui.crud;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.UUID;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.polypheny.db.PolyphenyDb;
import org.polypheny.db.TestHelper;
import org.polypheny.db.TestHelper.JdbcConnection;
import org.polypheny.db.catalog.Catalog;
import org.polypheny.db.catalog.mapping.RelMappingState;
import org.polypheny.db.ddl.DdlManager;
import org.polypheny.db.ddl.RelMappingUpdate;
import org.polypheny.db.ddl.RelMappingUpdate.ColumnName;
import org.polypheny.db.type.PolyType;
import org.polypheny.db.webui.models.mapping.MappingModel;


@Tag("adapter")
class RelMappingConversionValidationIntegrationTest {

    @BeforeAll
    static void startDatabase() {
        boolean autoDockerDisabled = PolyphenyDb.noAutoDocker;
        try {
            PolyphenyDb.noAutoDocker = true;
            TestHelper.getInstance();
        } finally {
            PolyphenyDb.noAutoDocker = autoDockerDisabled;
        }
    }


    @Test
    void checksRawSourceForDraftsAndSavedFailingMappingsWithoutChangingThem( @TempDir Path directory ) throws Exception {
        String suffix = UUID.randomUUID().toString().replace( "-", "" ).substring( 0, 8 );
        String adapterName = "checkcsv" + suffix;
        String sourceName = "checkfile" + suffix;
        Files.writeString( directory.resolve( sourceName + ".csv" ), "id:long,amount:string\n"
                + IntStream.rangeClosed( 1, 11 ).mapToObj( i -> i + "," + (i == 3 || i == 7 || i == 11 ? "bad" + i : i) + "\n" ).collect( Collectors.joining() ) );
        String escapedDirectory = directory.toString().replace( "\\", "\\\\" );
        try {
            execute( "ALTER ADAPTERS ADD \"" + adapterName + "\" USING 'CSV' AS 'Source' WITH '"
                    + "{\"method\":\"link\",\"directoryName\":\"" + escapedDirectory + "\",\"directory\":\"" + escapedDirectory + "\",\"maxStringLength\":\"40\"}'" );
            long adapterId = Catalog.snapshot().getAdapter( adapterName ).orElseThrow().id;
            var source = Catalog.snapshot().rel().getTable( Catalog.DEFAULT_NAMESPACE_NAME, sourceName ).orElseThrow();
            var draft = RelMappingState.draft( Catalog.getInstance(), adapterId, source.id );
            var amount = draft.columns().stream().filter( column -> column.name.equals( "amount" ) ).findFirst().orElseThrow();
            var conversion = new ColumnName( amount.id, amount.name, true, PolyType.INTEGER, true, null, null, "CONVERT_COLUMN" );
            int mappingsBefore = Catalog.snapshot().mappings().getMappings().size();
            var result = check( draft, conversion );
            assertEquals( 10, result.checkedRecords() );
            assertEquals( List.of( 3, 7 ), result.errors().stream().map( error -> error.recordNumber() ).toList() );
            assertEquals( mappingsBefore, Catalog.snapshot().mappings().getMappings().size() );
            assertEquals( draft.revision(), RelMappingState.draft( Catalog.getInstance(), adapterId, source.id ).revision() );

            var columns = draft.columns().stream().map( column -> column.id == amount.id ? conversion : new ColumnName( column.id, column.name, true ) ).toList();
            var command = new RelMappingUpdate( adapterId, -1, draft.revision(), draft.mapping().name(), draft.table().name, columns, draft.namespace().id );
            var transaction = TestHelper.getInstance().getTransaction();
            RelMappingState saved;
            try {
                saved = DdlManager.getInstance().createRelMapping( source.id, command, transaction.createStatement() );
            } finally {
                if ( transaction.isActive() ) {
                    transaction.rollback( "Conversion validation test cleanup" );
                }
            }
            var savedAmount = saved.columns().stream().filter( column -> column.name.equals( "amount" ) ).findFirst().orElseThrow();
            var savedResult = check( saved, new ColumnName( savedAmount.id, savedAmount.name, true ) );
            assertEquals( List.of( 3, 7 ), savedResult.errors().stream().map( error -> error.recordNumber() ).toList() );
            var repaired = check( saved, new ColumnName( savedAmount.id, savedAmount.name, true, PolyType.VARCHAR, true, 40, null, "CONVERT_COLUMN" ) );
            assertEquals( 10, repaired.checkedRecords() );
            assertTrue( repaired.errors().isEmpty() );
            assertEquals( saved.revision(), RelMappingState.load( Catalog.getInstance(), saved.mapping().id() ).revision() );
        } finally {
            if ( Catalog.snapshot().getAdapter( adapterName ).isPresent() ) {
                execute( "ALTER ADAPTERS DROP \"" + adapterName + "\"" );
            }
        }
    }


    private static MappingModel.ColumnCheck check( RelMappingState state, ColumnName column ) {
        var transaction = TestHelper.getInstance().getTransaction();
        try {
            return RelMappingConversionValidator.validate( state, List.of( column ), transaction.createStatement() ).columns().get( 0 );
        } finally {
            if ( transaction.isActive() ) {
                transaction.rollback( "Conversion validation test finished" );
            }
        }
    }


    private static void execute( String sql ) throws Exception {
        try ( JdbcConnection jdbc = new JdbcConnection( false ); var statement = jdbc.getConnection().createStatement() ) {
            statement.execute( sql );
            jdbc.getConnection().commit();
        }
    }

}
