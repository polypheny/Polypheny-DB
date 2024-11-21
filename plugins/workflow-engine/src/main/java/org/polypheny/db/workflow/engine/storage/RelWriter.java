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

package org.polypheny.db.workflow.engine.storage;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.StringJoiner;
import org.polypheny.db.algebra.type.AlgDataType;
import org.polypheny.db.algebra.type.AlgDataTypeField;
import org.polypheny.db.catalog.entity.logical.LogicalTable;
import org.polypheny.db.catalog.exceptions.GenericRuntimeException;
import org.polypheny.db.languages.LanguageManager;
import org.polypheny.db.languages.QueryLanguage;
import org.polypheny.db.processing.ImplementationContext;
import org.polypheny.db.processing.ImplementationContext.ExecutedContext;
import org.polypheny.db.processing.QueryContext;
import org.polypheny.db.transaction.Statement;
import org.polypheny.db.transaction.TransactionManager;
import org.polypheny.db.type.entity.PolyValue;
import org.polypheny.db.type.entity.numerical.PolyLong;

public class RelWriter extends CheckpointWriter {

    private final boolean resetPk;
    private long currentPk = 0;

    private final AlgDataType[] dataTypes;
    private final Statement statement;
    private final ImplementationContext implementation;
    private final Map<Long, AlgDataType> paramTypes = new HashMap<>();
    private final List<Map<Long, PolyValue>> paramValues = new ArrayList<>();
    private long batchSize = -1;


    public RelWriter( LogicalTable table, TransactionManager transactionManager, boolean resetPk ) {
        super( table, transactionManager );
        this.resetPk = resetPk;

        dataTypes = entity.getTupleType().getFields().stream().map( AlgDataTypeField::getType ).toArray( AlgDataType[]::new );
        statement = transaction.createStatement();

        StringJoiner joiner = new StringJoiner( ", ", "(", ")" );
        for ( int i = 0; i < table.getTupleType().getFieldCount(); i++ ) {
            joiner.add( "?" );
            AlgDataType fieldType = table.getTupleType().getFields().get( i ).getType();
            paramTypes.put( (long) i, fieldType );
        }

        String query = "INSERT INTO \"" + table.getName() + "\" VALUES " + joiner;
        QueryContext context = QueryContext.builder()
                .query( query )
                .language( QueryLanguage.from( "SQL" ) )
                .isAnalysed( false )
                .origin( StorageManager.ORIGIN )
                .namespaceId( table.getNamespaceId() )
                .transactionManager( transactionManager )
                .transactions( List.of( transaction ) ).build();
        implementation = LanguageManager.getINSTANCE().anyPrepareQuery( context, statement ).get( 0 );


    }


    public void write( PolyValue[] row, PolyValue appendValue ) {
        PolyValue[] appended = new PolyValue[row.length + 1];
        System.arraycopy( row, 0, appended, 0, row.length );
        appended[row.length] = appendValue;
        write( appended );
    }


    public void wInserted( PolyValue[] row, PolyValue insertValue, int insertIdx ) {
        PolyValue[] inserted = new PolyValue[row.length + 1];
        System.arraycopy( row, 0, inserted, 0, insertIdx );
        inserted[insertIdx] = insertValue;
        System.arraycopy( row, insertIdx, inserted, insertIdx + 1, row.length - insertIdx );
        write( inserted );
    }


    public void wReplaced( PolyValue[] row, PolyValue replaceValue, int replaceIdx ) {
        PolyValue[] replaced = row.clone();
        replaced[replaceIdx] = replaceValue;
        write( replaced );
    }


    public void wReplacedInPlace( PolyValue[] row, PolyValue replaceValue, int replaceIdx ) {
        row[replaceIdx] = replaceValue;
        write( row );
    }


    public void wRemoved( PolyValue[] row, int removeIdx ) {
        PolyValue[] removed = new PolyValue[row.length - 1];
        System.arraycopy( row, 0, removed, 0, removeIdx );
        System.arraycopy( row, removeIdx + 1, removed, removeIdx, row.length - removeIdx - 1 );
        write( removed );
    }


    public void write( PolyValue value ) {
        write( new PolyValue[]{ value } );
    }


    @Override
    public void write( PolyValue[] row ) {
        if ( resetPk ) {
            row[0] = PolyLong.of( currentPk );
            currentPk++;
        }
        if ( batchSize == -1 ) {
            batchSize = computeBatchSize( row );
        }
        paramValues.add( getParamMap( row ) );
        executeIfBatchFull();
    }


    @Override
    public void close() throws Exception {
        if ( !paramValues.isEmpty() ) {
            executeBatch();
        }
        super.close();
    }


    private void executeIfBatchFull() {
        if ( paramValues.size() < batchSize ) { // TODO: correctly determine threshold
            System.out.println( "Batch is not yet full (" + paramValues.size() + " of " + batchSize + ")" );
            return;
        }
        System.out.println( "batch is full, writing " + paramValues );
        executeBatch();
    }


    private void executeBatch() {
        int batchSize = paramValues.size();

        statement.getDataContext().setParameterTypes( paramTypes );
        statement.getDataContext().setParameterValues( paramValues );

        ExecutedContext executedContext = implementation.execute( statement );

        if ( executedContext.getException().isPresent() ) {
            throw new GenericRuntimeException( "An error occured while writing to the checkpoint" );
        }
        List<List<PolyValue>> results = executedContext.getIterator().getAllRowsAndClose();
        long changedCount = results.size() == 1 ? results.get( 0 ).get( 0 ).asLong().longValue() : 0;
        if ( changedCount != batchSize ) {
            throw new GenericRuntimeException( "Unable to write batch to checkpoint: " + changedCount + " of " + batchSize + " tuples were written" );
        }

        paramValues.clear();
        statement.getDataContext().resetParameterValues();
    }


    private Map<Long, PolyValue> getParamMap( PolyValue[] row ) {
        Map<Long, PolyValue> map = new HashMap<>();
        for ( int i = 0; i < row.length; i++ ) {
            map.put( (long) i, row[i] );
        }
        return map;
    }


    private LogicalTable getTable() {
        return (LogicalTable) entity;
    }


}
