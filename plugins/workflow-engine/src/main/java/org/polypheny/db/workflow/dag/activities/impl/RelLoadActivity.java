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
import java.util.List;
import java.util.Map;
import java.util.StringJoiner;
import org.apache.commons.lang3.NotImplementedException;
import org.polypheny.db.algebra.AlgNode;
import org.polypheny.db.algebra.type.AlgDataType;
import org.polypheny.db.catalog.entity.logical.LogicalTable;
import org.polypheny.db.catalog.logistic.DataModel;
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
import org.polypheny.db.workflow.dag.activities.TypePreview;
import org.polypheny.db.workflow.dag.annotations.ActivityDefinition;
import org.polypheny.db.workflow.dag.annotations.ActivityDefinition.InPort;
import org.polypheny.db.workflow.dag.annotations.EntitySetting;
import org.polypheny.db.workflow.dag.settings.EntityValue;
import org.polypheny.db.workflow.dag.settings.SettingDef.Settings;
import org.polypheny.db.workflow.dag.settings.SettingDef.SettingsPreview;
import org.polypheny.db.workflow.engine.execution.context.ExecutionContext;
import org.polypheny.db.workflow.engine.execution.context.PipeExecutionContext;
import org.polypheny.db.workflow.engine.execution.pipe.InputPipe;
import org.polypheny.db.workflow.engine.execution.pipe.OutputPipe;
import org.polypheny.db.workflow.engine.storage.BatchWriter;
import org.polypheny.db.workflow.engine.storage.QueryUtils;
import org.polypheny.db.workflow.engine.storage.reader.CheckpointReader;
import org.polypheny.db.workflow.engine.storage.reader.RelReader;

@ActivityDefinition(type = "relLoad", displayName = "Load Table", categories = { ActivityCategory.LOAD, ActivityCategory.RELATIONAL },
        inPorts = { @InPort(type = PortType.REL) },
        outPorts = {})

@EntitySetting(key = TABLE_KEY, displayName = "Table", dataModel = DataModel.RELATIONAL)
// add setting to keep or remove primary key column
// add setting to switch between insert and update
@SuppressWarnings("unused")
public class RelLoadActivity implements Activity, Fusable, Pipeable {

    static final String TABLE_KEY = "table";


    @Override
    public List<TypePreview> previewOutTypes( List<TypePreview> inTypes, SettingsPreview settings ) throws ActivityException {
        return List.of();
    }


    @Override
    public void execute( List<CheckpointReader> inputs, Settings settings, ExecutionContext ctx ) throws Exception {
        LogicalTable table = getEntity( settings.get( TABLE_KEY, EntityValue.class ) );
        write( table, ctx.getTransaction(), inputs.get( 0 ).getIterable(), ctx, null, ((RelReader) inputs.get( 0 )).getRowCount() );
    }


    @Override
    public AlgDataType lockOutputType( List<AlgDataType> inTypes, Settings settings ) throws Exception {
        return null;
    }


    @Override
    public void pipe( List<InputPipe> inputs, OutputPipe output, Settings settings, PipeExecutionContext ctx ) throws Exception {
        long estimatedTupleCount = estimateTupleCount( inputs.stream().map( InputPipe::getType ).toList(), settings, ctx.getEstimatedInCounts(), ctx::getTransaction );

        LogicalTable table = getEntity( settings.get( TABLE_KEY, EntityValue.class ) );
        write( table, ctx.getTransaction(), inputs.get( 0 ), null, ctx, estimatedTupleCount ); // we do not know the number of rows
    }


    @Override
    public AlgNode fuse( List<AlgNode> inputs, Settings settings, AlgCluster cluster ) throws Exception {
        throw new NotImplementedException();
    }


    private LogicalTable getEntity( EntityValue setting ) throws ActivityException {
        // TODO: check if the adapter is a data store (and thus writable)
        LogicalTable table = setting.getTable();
        if ( table == null ) {
            throw new InvalidSettingException( "Specified table does not exist", TABLE_KEY );
        }
        return table;
    }


    private void write( LogicalTable table, Transaction transaction, Iterable<List<PolyValue>> rows, ExecutionContext ctx, PipeExecutionContext pipeCtx, long totalRows ) throws Exception {
        assert ctx != null || pipeCtx != null;

        Map<Long, AlgDataType> paramTypes = new HashMap<>();
        StringJoiner joiner = new StringJoiner( ", ", "(", ")" );
        for ( int i = 1; i < table.getTupleType().getFieldCount(); i++ ) {
            joiner.add( "?" );
            AlgDataType fieldType = table.getTupleType().getFields().get( i ).getType();
            paramTypes.put( (long) i - 1, fieldType );
        }

        String query = "INSERT INTO \"" + table.getName() + "\" VALUES " + joiner;
        QueryContext context = QueryUtils.constructContext( query, "SQL", table.getNamespaceId(), transaction );

        int mapCapacity = (int) Math.ceil( table.getTupleType().getFieldCount() / 0.75 );
        long rowCount = 0;
        long countDelta = Math.max( totalRows / 100, 1 );
        try ( BatchWriter writer = new BatchWriter( context, transaction.createStatement(), paramTypes ) ) {
            for ( List<PolyValue> row : rows ) {
                Map<Long, PolyValue> map = new HashMap<>( mapCapacity );
                for ( int i = 1; i < row.size(); i++ ) { // skip primary key
                    map.put( (long) i - 1, row.get( i ) );
                }
                writer.write( map );

                rowCount++;

                if ( rowCount % countDelta == 0 ) {
                    double progress = (double) rowCount / totalRows;
                    if ( ctx != null ) {
                        ctx.updateProgress( progress );
                        ctx.checkInterrupted();
                    } else {
                        pipeCtx.updateProgress( progress );
                    }
                }
            }
        }
    }

}
