/*
 * Copyright 2019-2020 The Polypheny Project
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

package org.polypheny.db.adapter.multimedia;


import java.io.File;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;
import org.apache.calcite.linq4j.AbstractEnumerable;
import org.apache.calcite.linq4j.Enumerable;
import org.apache.calcite.linq4j.Enumerator;
import org.apache.calcite.linq4j.Queryable;
import org.polypheny.db.adapter.DataContext;
import org.polypheny.db.adapter.java.AbstractQueryableTable;
import org.polypheny.db.plan.Convention;
import org.polypheny.db.plan.RelOptCluster;
import org.polypheny.db.plan.RelOptTable;
import org.polypheny.db.prepare.Prepare.CatalogReader;
import org.polypheny.db.rel.RelNode;
import org.polypheny.db.rel.core.TableModify;
import org.polypheny.db.rel.core.TableModify.Operation;
import org.polypheny.db.rel.logical.LogicalTableModify;
import org.polypheny.db.rel.type.RelDataType;
import org.polypheny.db.rel.type.RelDataTypeFactory;
import org.polypheny.db.rex.RexNode;
import org.polypheny.db.schema.ModifiableTable;
import org.polypheny.db.schema.ScannableTable;
import org.polypheny.db.schema.SchemaPlus;
import org.polypheny.db.schema.impl.AbstractTableQueryable;
import org.polypheny.db.type.PolyType;


public class MultimediaTable extends AbstractQueryableTable implements ScannableTable, ModifiableTable {
    //extends AbstractQueryableTable
    //implements TranslatableTable, ModifiableTable

    final File rootDir;
    private final String schemaName;
    long tableId;
    List<Long> columnIds;
    List<PolyType> columnTypes;
    List<String> columnNames;
    MultimediaStore store;


    public MultimediaTable( final File rootDir, String schemaName, long tableId, List<Long> columnIds, ArrayList<PolyType> columnTypes, List<String> columnNames, MultimediaStore store ) {
        super( Object[].class );
        this.rootDir = rootDir;
        this.schemaName = schemaName;
        this.tableId = tableId;
        this.columnIds = columnIds;
        this.columnTypes = columnTypes;
        this.columnNames = columnNames;
        this.store = store;
    }

    @Override
    public RelDataType getRowType( RelDataTypeFactory typeFactory ) {
        final RelDataTypeFactory.Builder fieldInfo = typeFactory.builder();
        for( int i = 0; i < columnIds.size(); i++ ) {
            fieldInfo.add( columnNames.get( i ), columnNames.get( i ), columnTypes.get( i ) );
        }
        return fieldInfo.build();
    }

    @Override
    public Enumerable<Object[]> scan( DataContext root ) {
        //todo
        root.getStatement().getTransaction().registerInvolvedStore( store );
        final AtomicBoolean cancelFlag = DataContext.Variable.CANCEL_FLAG.get( root );
        return new AbstractEnumerable<Object[]>() {
            @Override
            public Enumerator<Object[]> enumerator() {
                return new MultimediaEnumerator<>( store, schemaName, tableId, columnIds, columnTypes, cancelFlag );
            }
        };
    }


    @Override
    public TableModify toModificationRel( RelOptCluster cluster, RelOptTable table, CatalogReader catalogReader, RelNode child, Operation operation, List<String> updateColumnList, List<RexNode> sourceExpressionList, boolean flattened ) {
        //todo getconvention -> register planner
        //cassandraSchema.getConvention().register( cluster.getPlanner() );
        return new LogicalTableModify( cluster, cluster.traitSetOf( Convention.NONE ), table, catalogReader, child, operation, updateColumnList, sourceExpressionList, flattened );
    }

    @Override
    public Collection getModifiableCollection() {
        //see SqlCreateTable.java ..
        throw new RuntimeException( "getModifiableCollection() is not implemented for Multimedia adapter!" );
    }

    @Override
    public <T> Queryable<T> asQueryable( DataContext dataContext, SchemaPlus schema, String tableName ) {
        //with Linq4j: see SqlCreateTable.java
        return new MultimediaQueryable<>( dataContext, schema, this, tableName );
    }

    public class MultimediaQueryable<T> extends AbstractTableQueryable<T> {

        public MultimediaQueryable ( DataContext dataContext, SchemaPlus schema, MultimediaTable table, String tableName ) {
            super( dataContext, schema, MultimediaTable.this, tableName );
        }

        @Override
        public Enumerator<T> enumerator() {
            //todo check
            //for Linq4j: see SqlCreateTable.java ..
            return new MultimediaEnumerator<>( store, schemaName, tableId, columnIds, columnTypes, dataContext.getStatement().getTransaction().getCancelFlag() );
        }

    }

}
