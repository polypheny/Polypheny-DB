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

package org.polypheny.db.adapter.file;


import java.io.File;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import lombok.Getter;
import org.apache.calcite.linq4j.AbstractEnumerable;
import org.apache.calcite.linq4j.Enumerable;
import org.apache.calcite.linq4j.Enumerator;
import org.apache.calcite.linq4j.tree.Expression;
import org.jetbrains.annotations.Nullable;
import org.polypheny.db.adapter.Adapter;
import org.polypheny.db.adapter.AdapterManager;
import org.polypheny.db.adapter.DataContext;
import org.polypheny.db.adapter.file.FileAlg.FileImplementor.Operation;
import org.polypheny.db.catalog.entity.physical.PhysicalTable;
import org.polypheny.db.schema.Namespace;
import org.polypheny.db.type.entity.PolyValue;


@Getter
public class FileStoreSchema extends Namespace implements FileSchema {

    private final String schemaName;
    @Getter
    private final FileStore store;
    private final FileConvention convention;


    public FileStoreSchema( long id, long adapterId, String schemaName, FileStore store ) {
        super( id, adapterId );
        this.schemaName = schemaName;
        this.store = store;
        final Expression expression = null;
        this.convention = new FileConvention( schemaName, expression, this );
    }


    @Override
    public File getRootDir() {
        return store.getRootDir();
    }


    @Override
    public Adapter<?> getAdapter() {
        return store;
    }



    public FileTranslatableEntity createFileTable( PhysicalTable table, List<Long> primary ) {
        return new FileTranslatableEntity(
                this,
                table,
                primary );
    }


    /**
     * Called from generated code
     * Executes SELECT, UPDATE and DELETE operations
     * see {@link FileMethod#EXECUTE} and
     */
    @SuppressWarnings("unused")
    public static Enumerable<PolyValue[]> execute(
            final Operation operation,
            final Long adapterId,
            final Long allocId,
            final DataContext dataContext,
            final String path,
            final Long[] columnIds,
            final FileTranslatableEntity entity,
            final List<Long> pkIds,
            final @Nullable List<Value> projectionMapping,
            final Condition condition,
            final List<List<PolyValue>> updates ) {
        dataContext.getStatement().getTransaction().registerInvolvedAdapter( AdapterManager.getInstance().getAdapter( adapterId ).orElseThrow() );
        return new AbstractEnumerable<>() {
            @Override
            public Enumerator<PolyValue[]> enumerator() {
                return new FileEnumerator( operation, path, allocId, columnIds, entity, pkIds, projectionMapping, dataContext, condition, updates );
            }
        };
    }


    /**
     * Called from generated code
     * Executes INSERT operations
     * see {@link FileMethod#EXECUTE_MODIFY} and
     */
    @SuppressWarnings("unused")
    public static Enumerable<PolyValue[]> executeModify(
            final Operation operation,
            final Long adapterId,
            final Long allocId,
            final DataContext dataContext,
            final String path,
            final Long[] columnIds,
            final FileTranslatableEntity entity,
            final List<Long> pkIds,
            final Boolean isBatch,
            final List<List<PolyValue>> insertValues,
            final Condition condition ) {
        dataContext.getStatement().getTransaction().registerInvolvedAdapter( AdapterManager.getInstance().getAdapter( adapterId ).orElseThrow() );
        final List<List<PolyValue>> insert;

        List<List<PolyValue>> rows = new ArrayList<>();
        int i = 0;
        if ( !dataContext.getParameterValues().isEmpty() ) {
            for ( Map<Long, PolyValue> map : dataContext.getParameterValues() ) {
                List<PolyValue> row = new ArrayList<>();
                //insertValues[] has length 1 if the dataContext is set
                for ( PolyValue values : insertValues.get( 0 ) ) {
                    row.add( ((Value) values).getValue( null, dataContext, i ) );
                }
                rows.add( row );
                i++;
            }
        } else {
            for ( List<PolyValue> values : insertValues ) {
                List<PolyValue> row = new ArrayList<>();
                for ( PolyValue value : values ) {
                    row.add( ((Value) value).getValue( null, dataContext, i ) );
                }
                rows.add( row );
                i++;
            }
        }
        insert = rows;

        return new AbstractEnumerable<>() {
            @Override
            public Enumerator<PolyValue[]> enumerator() {
                return new FileModifier( operation, path, allocId, columnIds, entity, pkIds, dataContext, insert, condition );
            }
        };
    }

}
