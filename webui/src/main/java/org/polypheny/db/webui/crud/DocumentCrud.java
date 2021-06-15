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

package org.polypheny.db.webui.crud;

import java.sql.ResultSetMetaData;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import lombok.extern.slf4j.Slf4j;
import org.apache.calcite.avatica.ColumnMetaData;
import org.apache.calcite.avatica.MetaImpl;
import org.apache.calcite.linq4j.Enumerable;
import org.apache.commons.lang3.time.StopWatch;
import org.polypheny.db.catalog.Catalog;
import org.polypheny.db.catalog.entity.CatalogColumn;
import org.polypheny.db.catalog.entity.CatalogTable;
import org.polypheny.db.catalog.exceptions.UnknownColumnException;
import org.polypheny.db.catalog.exceptions.UnknownDatabaseException;
import org.polypheny.db.catalog.exceptions.UnknownSchemaException;
import org.polypheny.db.catalog.exceptions.UnknownTableException;
import org.polypheny.db.jdbc.PolyphenyDbSignature;
import org.polypheny.db.mql.Mql.Family;
import org.polypheny.db.mql.MqlNode;
import org.polypheny.db.processing.MqlProcessor;
import org.polypheny.db.rel.RelRoot;
import org.polypheny.db.transaction.Statement;
import org.polypheny.db.webui.Crud;
import org.polypheny.db.webui.models.DbColumn;
import org.polypheny.db.webui.models.Result;
import org.polypheny.db.webui.models.SortState;
import org.polypheny.db.webui.models.requests.QueryRequest;

@Slf4j
public class DocumentCrud {

    private final Crud crud;


    public DocumentCrud( Crud crud ) {
        this.crud = crud;
    }


    public List<Result> anyQuery( Statement statement, String mql, QueryRequest request ) {
        PolyphenyDbSignature<?> signature;
        Catalog catalog = Catalog.getInstance();
        MqlProcessor mqlProcessor = statement.getTransaction().getMqlProcessor();

        ArrayList<Result> results = new ArrayList<>();

        MqlNode parsed = mqlProcessor.parse( mql );

        List<List<Object>> rows;
        Iterator<Object> iterator = null;
        boolean hasMoreRows = false;

        if ( parsed.getFamily() == Family.DDL ) {
            mqlProcessor.prepareDdl( statement, parsed, mql );
            Result result = new Result( 1 ).setGeneratedQuery( mql ).setXid( statement.getTransaction().getXid().toString() );
            results.add( result );
        } else {
            //Pair<SqlNode, RelDataType> validated = mqlProcessor.validate( statement.getTransaction(), parsed ); TODO DL
            RelRoot logicalRoot = mqlProcessor.translate( statement, parsed );

            // Prepare
            signature = statement.getQueryProcessor().prepareQuery( logicalRoot );
            final Enumerable enumerable = signature.enumerable( statement.getDataContext() );
            //noinspection unchecked
            iterator = enumerable.iterator();
            StopWatch stopWatch = new StopWatch();
            stopWatch.start();

            rows = MetaImpl.collect( signature.cursorFactory, iterator, new ArrayList<>() );

            hasMoreRows = iterator.hasNext();
            stopWatch.stop();
            signature.getExecutionTimeMonitor().setExecutionTime( stopWatch.getNanoTime() );

            try {
                CatalogTable catalogTable = null;
                if ( request.tableId != null ) {
                    String[] t = request.tableId.split( "\\." );
                    try {
                        catalogTable = Catalog.getInstance().getTable( statement.getPrepareContext().getDefaultSchemaName(), t[0], t[1] );
                    } catch ( UnknownTableException | UnknownDatabaseException | UnknownSchemaException e ) {
                        log.error( "Caught exception", e );
                    }
                }

                ArrayList<DbColumn> header = new ArrayList<>();
                for ( ColumnMetaData metaData : signature.columns ) {
                    String columnName = metaData.columnName;

                    String filter = "";
                    if ( request.filter != null && request.filter.containsKey( columnName ) ) {
                        filter = request.filter.get( columnName );
                    }

                    SortState sort;
                    if ( request.sortState != null && request.sortState.containsKey( columnName ) ) {
                        sort = request.sortState.get( columnName );
                    } else {
                        sort = new SortState();
                    }

                    DbColumn dbCol = new DbColumn(
                            metaData.columnName,
                            metaData.type.name,
                            metaData.nullable == ResultSetMetaData.columnNullable,
                            metaData.displaySize,
                            sort,
                            filter );

                    // Get column default values
                    if ( catalogTable != null ) {
                        try {
                            if ( catalog.checkIfExistsColumn( catalogTable.id, columnName ) ) {
                                CatalogColumn catalogColumn = catalog.getColumn( catalogTable.id, columnName );
                                if ( catalogColumn.defaultValue != null ) {
                                    dbCol.defaultValue = catalogColumn.defaultValue.value;
                                }
                            }
                        } catch ( UnknownColumnException e ) {
                            log.error( "Caught exception", e );
                        }
                    }
                    header.add( dbCol );
                }

                ArrayList<String[]> data = crud.computeResultData( rows, header, statement.getTransaction() );

                return Collections.singletonList( new Result( header.toArray( new DbColumn[0] ), data.toArray( new String[0][] ) ).setAffectedRows( data.size() ).setHasMoreRows( hasMoreRows ) );
            } finally {
                try {
                    ((AutoCloseable) iterator).close();
                } catch ( Exception e ) {
                    log.error( "Exception while closing result iterator", e );
                }
            }
        }

        return results;
    }

}
