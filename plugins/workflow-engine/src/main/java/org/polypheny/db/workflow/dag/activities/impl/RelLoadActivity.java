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

package org.polypheny.db.workflow.dag.activities.impl;

import static org.polypheny.db.workflow.dag.activities.impl.RelLoadActivity.TABLE_KEY;

import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.StringJoiner;
import org.apache.commons.lang3.NotImplementedException;
import org.polypheny.db.algebra.AlgNode;
import org.polypheny.db.algebra.type.AlgDataType;
import org.polypheny.db.catalog.Catalog;
import org.polypheny.db.catalog.entity.logical.LogicalTable;
import org.polypheny.db.catalog.logistic.DataModel;
import org.polypheny.db.languages.QueryLanguage;
import org.polypheny.db.plan.AlgCluster;
import org.polypheny.db.processing.QueryContext;
import org.polypheny.db.transaction.Transaction;
import org.polypheny.db.type.entity.PolyValue;
import org.polypheny.db.workflow.dag.activities.Activity;
import org.polypheny.db.workflow.dag.activities.Activity.ActivityCategory;
import org.polypheny.db.workflow.dag.activities.Activity.PortType;
import org.polypheny.db.workflow.dag.activities.ActivityException;
import org.polypheny.db.workflow.dag.activities.ActivityException.InvalidSettingException;
import org.polypheny.db.workflow.dag.activities.Fusable;
import org.polypheny.db.workflow.dag.activities.Pipeable;
import org.polypheny.db.workflow.dag.annotations.ActivityDefinition;
import org.polypheny.db.workflow.dag.annotations.ActivityDefinition.InPort;
import org.polypheny.db.workflow.dag.annotations.EntitySetting;
import org.polypheny.db.workflow.dag.settings.EntityValue;
import org.polypheny.db.workflow.dag.settings.SettingDef.SettingValue;
import org.polypheny.db.workflow.engine.execution.context.ExecutionContext;
import org.polypheny.db.workflow.engine.execution.context.PipeExecutionContext;
import org.polypheny.db.workflow.engine.execution.pipe.InputPipe;
import org.polypheny.db.workflow.engine.execution.pipe.OutputPipe;
import org.polypheny.db.workflow.engine.storage.QueryUtils.BatchWriter;
import org.polypheny.db.workflow.engine.storage.StorageManager;
import org.polypheny.db.workflow.engine.storage.reader.CheckpointReader;
import org.polypheny.db.workflow.engine.storage.writer.RelWriter;

@ActivityDefinition(type = "relLoad", displayName = "Load Table", categories = { ActivityCategory.LOAD },
        inPorts = { @InPort(type = PortType.REL) },
        outPorts = {})

@EntitySetting(key = TABLE_KEY, displayName = "Table", dataModel = DataModel.RELATIONAL)
// add setting to keep or remove primary key column
// add setting to switch between insert and update
public class RelLoadActivity implements Activity, Fusable, Pipeable {

    static final String TABLE_KEY = "table";


    @Override
    public List<Optional<AlgDataType>> previewOutTypes( List<Optional<AlgDataType>> inTypes, Map<String, Optional<SettingValue>> settings ) throws ActivityException {
        return List.of();
    }


    @Override
    public void reset() {

    }


    @Override
    public void execute( List<CheckpointReader> inputs, Map<String, SettingValue> settings, ExecutionContext ctx ) throws Exception {
        LogicalTable table = getEntity( settings.get( TABLE_KEY ) );
        Transaction transaction = ctx.getTransaction();
        int mapCapacity = (int) Math.ceil( table.getTupleType().getFieldCount() / 0.75 );

        Map<Long, AlgDataType> paramTypes = new HashMap<>();
        StringJoiner joiner = new StringJoiner( ", ", "(", ")" );
        for ( int i = 1; i < table.getTupleType().getFieldCount(); i++ ) {
            joiner.add( "?" );
            AlgDataType fieldType = table.getTupleType().getFields().get( i ).getType();
            paramTypes.put( (long) i-1, fieldType );
        }

        String query = "INSERT INTO \"" + table.getName() + "\" VALUES " + joiner;
        QueryContext context = QueryContext.builder()
                .query( query )
                .language( QueryLanguage.from( "SQL" ) )
                .isAnalysed( false )
                .origin( StorageManager.ORIGIN + "-RelWriterCtx" )
                .namespaceId( table.getNamespaceId() )
                .transactionManager( transaction.getTransactionManager() )
                .transactions( List.of( transaction ) ).build();

        try(BatchWriter writer = new BatchWriter( context, transaction.createStatement(), paramTypes )) {
            Iterator<List<PolyValue>> it = inputs.get( 0 ).getIterator();
            while ( it.hasNext() ) {
                Map<Long, PolyValue> map = new HashMap<>( mapCapacity );
                List<PolyValue> row = it.next();
                for ( int i = 1; i < row.size(); i++ ) { // skip primary key
                    map.put( (long) i-1, row.get( i ) );
                }
                writer.write( map );
            }
        }

    }


    @Override
    public AlgDataType lockOutputType( List<AlgDataType> inTypes, Map<String, SettingValue> settings ) throws Exception {
        return null;
    }


    @Override
    public void pipe( List<InputPipe> inputs, OutputPipe output, Map<String, SettingValue> settings, PipeExecutionContext ctx ) throws Exception {
        LogicalTable table = getEntity( settings.get( TABLE_KEY ) );

        // TODO: modify writer to allow its use for arbitrary entities, not just checkpoints (requires disabling of auto commit etc.)
        try ( RelWriter writer = new RelWriter( table, ctx.getTransaction(), true ) ) {
            writer.write( inputs.get( 0 ).iterator() );
        }
    }


    @Override
    public AlgNode fuse( List<AlgNode> inputs, Map<String, SettingValue> settings, AlgCluster cluster ) throws Exception {
        throw new NotImplementedException();
    }


    private LogicalTable getEntity( SettingValue setting ) throws ActivityException {
        EntityValue entitySetting = (EntityValue) setting;
        // TODO: check if the adapter is a data store (and thus writable)
        return Catalog.snapshot().rel().getTable( entitySetting.getNamespace(), entitySetting.getName() ).orElseThrow(
                () -> new InvalidSettingException( "Specified table does not exist", "table" ) );
    }

}
