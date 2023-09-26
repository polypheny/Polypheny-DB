/*
 * Copyright 2019-2023 The Polypheny Project
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
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import lombok.Getter;
import org.apache.calcite.linq4j.AbstractEnumerable;
import org.apache.calcite.linq4j.Enumerable;
import org.apache.calcite.linq4j.Enumerator;
import org.apache.calcite.linq4j.tree.Expression;
import org.polypheny.db.adapter.AdapterManager;
import org.polypheny.db.adapter.DataContext;
import org.polypheny.db.adapter.file.FileAlg.FileImplementor.Operation;
import org.polypheny.db.adapter.file.FilePlugin.FileStore;
import org.polypheny.db.adapter.file.util.FileUtil;
import org.polypheny.db.catalog.entity.CatalogEntity;
import org.polypheny.db.catalog.entity.physical.PhysicalTable;
import org.polypheny.db.schema.Namespace.Schema;
import org.polypheny.db.schema.impl.AbstractNamespace;
import org.polypheny.db.type.PolyType;
import org.polypheny.db.type.entity.PolyValue;


@Getter
public class FileStoreSchema extends AbstractNamespace implements FileSchema, Schema {

    private final String schemaName;
    private final Map<String, CatalogEntity> tables = new HashMap<>();
    private final FileStore store;
    private final FileConvention convention;


    public FileStoreSchema( long id, String schemaName, FileStore store ) {
        super( id );
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
    public Long getAdapterId() {
        return store.getAdapterId();
    }


    public FileTranslatableEntity createFileTable( PhysicalTable table ) {
        List<Long> pkIds;
        pkIds = new ArrayList<>();// todo dl
        return new FileTranslatableEntity(
                this,
                table,
                pkIds );
    }


    /**
     * Called from generated code
     * Executes SELECT, UPDATE and DELETE operations
     * see {@link FileMethod#EXECUTE} and {@link org.polypheny.db.adapter.file.algebra.FileToEnumerableConverter#implement}
     */
    @SuppressWarnings("unused")
    public static Enumerable<PolyValue[]> execute(
            final Operation operation,
            final Long adapterId,
            final Long allocId,
            final DataContext dataContext,
            final String path,
            final Long[] columnIds,
            final PolyType[] columnTypes,
            final List<Long> pkIds,
            final Integer[] projectionMapping,
            final Condition condition,
            final Value[] updates ) {
        dataContext.getStatement().getTransaction().registerInvolvedAdapter( AdapterManager.getInstance().getAdapter( adapterId ) );
        return new AbstractEnumerable<>() {
            @Override
            public Enumerator<PolyValue[]> enumerator() {
                return new FileEnumerator( operation, path, allocId, columnIds, columnTypes, pkIds, projectionMapping, dataContext, condition, updates );
            }
        };
    }


    /**
     * Called from generated code
     * Executes INSERT operations
     * see {@link FileMethod#EXECUTE_MODIFY} and {@link org.polypheny.db.adapter.file.algebra.FileToEnumerableConverter#implement}
     */
    @SuppressWarnings("unused")
    public static Enumerable<PolyValue[]> executeModify(
            final Operation operation,
            final Long adapterId,
            final Long allocId,
            final DataContext dataContext,
            final String path,
            final Long[] columnIds,
            final PolyType[] columnTypes,
            final List<Long> pkIds,
            final Boolean isBatch,
            final Object[] insertValues,
            final Condition condition ) {
        dataContext.getStatement().getTransaction().registerInvolvedAdapter( AdapterManager.getInstance().getAdapter( adapterId ) );
        final Object[] insert;

        List<Object[]> rows = new ArrayList<>();
        List<Object> row = new ArrayList<>();
        int i = 0;
        if ( !dataContext.getParameterValues().isEmpty() ) {
            for ( Map<Long, PolyValue> map : dataContext.getParameterValues() ) {
                row.clear();
                //insertValues[] has length 1 if the dataContext is set
                for ( Value values : ((Value[]) insertValues[0]) ) {
                    row.add( FileUtil.fromValue( values.getValue( dataContext, i ) ) );
                }
                rows.add( row.toArray( new Object[0] ) );
                i++;
            }
        } else {
            for ( Object insertRow : insertValues ) {
                row.clear();
                Value[] values = (Value[]) insertRow;
                for ( Value value : values ) {
                    row.add( value.getValue( dataContext, i ) );
                }
                rows.add( row.toArray( new Object[0] ) );
                i++;
            }
        }
        insert = rows.toArray( new Object[0] );

        return new AbstractEnumerable<>() {
            @Override
            public Enumerator<PolyValue[]> enumerator() {
                return new FileModifier( operation, path, allocId, columnIds, columnTypes, pkIds, dataContext, insert, condition );
            }
        };
    }

}
