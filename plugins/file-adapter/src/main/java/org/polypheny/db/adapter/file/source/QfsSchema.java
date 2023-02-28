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

package org.polypheny.db.adapter.file.source;


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
import org.polypheny.db.adapter.file.Condition;
import org.polypheny.db.adapter.file.FileAlg.FileImplementor.Operation;
import org.polypheny.db.adapter.file.FileConvention;
import org.polypheny.db.adapter.file.FileSchema;
import org.polypheny.db.adapter.file.FileTranslatableEntity;
import org.polypheny.db.adapter.file.Value;
import org.polypheny.db.catalog.Catalog;
import org.polypheny.db.catalog.Snapshot;
import org.polypheny.db.catalog.entity.CatalogPrimaryKey;
import org.polypheny.db.catalog.entity.allocation.AllocationTable;
import org.polypheny.db.catalog.entity.logical.LogicalTable;
import org.polypheny.db.schema.Namespace.Schema;
import org.polypheny.db.schema.Schemas;
import org.polypheny.db.schema.impl.AbstractNamespace;
import org.polypheny.db.type.PolyType;


public class QfsSchema extends AbstractNamespace implements FileSchema, Schema {

    @Getter
    private final String schemaName;
    private final Map<String, FileTranslatableEntity> tableMap = new HashMap<>();
    @Getter
    private final Qfs source;
    @Getter
    private final FileConvention convention;


    public QfsSchema( long id, Snapshot snapshot, String schemaName, Qfs source ) {
        super( id );
        this.schemaName = schemaName;
        this.source = source;
        final Expression expression = Schemas.subSchemaExpression( snapshot, schemaName, QfsSchema.class );
        this.convention = new QfsConvention( schemaName, expression, this );
    }


    @Override
    public File getRootDir() {
        return source.getRootDir();
    }


    @Override
    public int getAdapterId() {
        return source.getAdapterId();
    }


    public FileTranslatableEntity createFileTable( LogicalTable logicalTable, AllocationTable allocationTable ) {

        List<Long> pkIds;
        if ( logicalTable.primaryKey != null ) {
            CatalogPrimaryKey primaryKey = Catalog.getInstance().getPrimaryKey( logicalTable.primaryKey );
            pkIds = primaryKey.columnIds;
        } else {
            pkIds = new ArrayList<>();
        }
        FileTranslatableEntity table = new FileTranslatableEntity(
                this,
                logicalTable, allocationTable,
                pkIds );
        tableMap.put( logicalTable.name + "_" + allocationTable.name, table );
        return table;
    }


    /**
     * Called from generated code
     */
    @SuppressWarnings("unused")
    public static Enumerable<Object[]> execute(
            final Operation operation,
            final Integer adapterId,
            final Long partitionId,
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
            public Enumerator<Object[]> enumerator() {
                return new QfsEnumerator<>( dataContext, path, columnIds, projectionMapping, condition );
            }
        };
    }

}
