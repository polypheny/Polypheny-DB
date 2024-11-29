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

import static org.polypheny.db.workflow.dag.activities.impl.RelExtractActivity.TABLE_KEY;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.StringJoiner;
import org.apache.commons.lang3.NotImplementedException;
import org.polypheny.db.ResultIterator;
import org.polypheny.db.algebra.AlgNode;
import org.polypheny.db.algebra.type.AlgDataType;
import org.polypheny.db.algebra.type.AlgDataTypeFactory;
import org.polypheny.db.algebra.type.AlgDataTypeField;
import org.polypheny.db.catalog.Catalog;
import org.polypheny.db.catalog.entity.logical.LogicalTable;
import org.polypheny.db.catalog.logistic.DataModel;
import org.polypheny.db.languages.LanguageManager;
import org.polypheny.db.languages.QueryLanguage;
import org.polypheny.db.plan.AlgCluster;
import org.polypheny.db.processing.ImplementationContext.ExecutedContext;
import org.polypheny.db.processing.QueryContext;
import org.polypheny.db.transaction.Transaction;
import org.polypheny.db.type.PolyType;
import org.polypheny.db.workflow.dag.activities.Activity;
import org.polypheny.db.workflow.dag.activities.Activity.ActivityCategory;
import org.polypheny.db.workflow.dag.activities.Activity.PortType;
import org.polypheny.db.workflow.dag.activities.ActivityException;
import org.polypheny.db.workflow.dag.activities.ActivityException.InvalidSettingException;
import org.polypheny.db.workflow.dag.activities.Fusable;
import org.polypheny.db.workflow.dag.activities.Pipeable;
import org.polypheny.db.workflow.dag.annotations.ActivityDefinition;
import org.polypheny.db.workflow.dag.annotations.ActivityDefinition.OutPort;
import org.polypheny.db.workflow.dag.annotations.EntitySetting;
import org.polypheny.db.workflow.dag.settings.EntityValue;
import org.polypheny.db.workflow.dag.settings.SettingDef.SettingValue;
import org.polypheny.db.workflow.engine.execution.context.ExecutionContext;
import org.polypheny.db.workflow.engine.execution.context.PipeExecutionContext;
import org.polypheny.db.workflow.engine.execution.pipe.InputPipe;
import org.polypheny.db.workflow.engine.execution.pipe.OutputPipe;
import org.polypheny.db.workflow.engine.storage.QueryUtils;
import org.polypheny.db.workflow.engine.storage.StorageManager;
import org.polypheny.db.workflow.engine.storage.reader.CheckpointReader;
import org.polypheny.db.workflow.engine.storage.writer.RelWriter;

@ActivityDefinition(type = "relExtract", displayName = "Read Table", categories = { ActivityCategory.EXTRACT },
        inPorts = {},
        outPorts = { @OutPort(type = PortType.REL) })

@EntitySetting(key = TABLE_KEY, displayName = "Table", dataModel = DataModel.RELATIONAL)

public class RelExtractActivity implements Activity, Fusable, Pipeable {

    static final String TABLE_KEY = "table";

    private LogicalTable lockedEntity;


    @Override
    public List<Optional<AlgDataType>> previewOutTypes( List<Optional<AlgDataType>> inTypes, Map<String, Optional<SettingValue>> settings ) throws ActivityException {
        Optional<SettingValue> table = settings.get( TABLE_KEY );

        if ( table.isPresent() ) {
            AlgDataType type = getOutputType( getEntity( table.get() ) );
            return Activity.wrapType( type );
        }
        return Activity.wrapType( null );
    }


    @Override
    public void execute( List<CheckpointReader> inputs, Map<String, SettingValue> settings, ExecutionContext ctx ) throws Exception {
        LogicalTable table = getEntity( settings.get( TABLE_KEY ) );
        AlgDataType type = getOutputType( table );

        String quotedCols = QueryUtils.quoteAndJoin( table.getColumnNames() );
        String quotedName = QueryUtils.quotedIdentifier( table );
        String query = "SELECT 0, " + quotedCols + " FROM " + quotedName; // add a new column for the primary key



        /* TODO: This order sometimes results in a deadlock. Could be a Problem of hsqldb, since Postgres has no problem
        List<ExecutedContext> executedContexts = LanguageManager.getINSTANCE().anyQuery( context );
        try ( ResultIterator result = executedContexts.get( 0 ).getIterator();
                RelWriter writer = (RelWriter) ctx.createWriter( 0, type, true ) ) {
            writer.write( CheckpointReader.arrayToListIterator( result.getIterator(), true ) );
        }*/

        // 1. open tx 1
        // 2. 1 reads table A
        // 3. open tx 2
        // 4. create table B -> does not complete

        Map<Long, AlgDataType> paramTypes = new HashMap<>();
        StringJoiner joiner = new StringJoiner( ", ", "(", ")" );
        for ( int i = 0; i < table.getTupleType().getFieldCount(); i++ ) {
            joiner.add( "?" );
            AlgDataType fieldType = table.getTupleType().getFields().get( i ).getType();
            paramTypes.put( (long) i, fieldType );
        }

        try ( RelWriter writer = (RelWriter) ctx.createWriter( 0, type, true ) ) {
            Transaction transaction = ctx.getTransaction(); // transaction will get committed or rolled back externally
            QueryContext context = QueryContext.builder()
                    .query( query )
                    .language( QueryLanguage.from( "SQL" ) )
                    .isAnalysed( false )
                    .origin( StorageManager.ORIGIN )
                    .namespaceId( table.getNamespaceId() )
                    .transactionManager( transaction.getTransactionManager() )
                    .transactions( List.of( transaction ) ).build();
            List<ExecutedContext> executedContexts = LanguageManager.getINSTANCE().anyQuery( context );
            try ( ResultIterator result = executedContexts.get( 0 ).getIterator() ) {
                writer.write( CheckpointReader.arrayToListIterator( result.getIterator(), true ) );
            }

        }

    }


    @Override
    public void reset() {
        lockedEntity = null;
    }


    @Override
    public AlgNode fuse( List<AlgNode> inputs, Map<String, SettingValue> settings, AlgCluster cluster ) throws Exception {
        throw new NotImplementedException();
        /*LogicalTable table = getEntity( settings.get( TABLE_KEY ) );
        AlgDataType type = getOutputType( table );
        AlgTraitSet traits = AlgTraitSet.createEmpty().plus( ModelTrait.RELATIONAL );

        AlgNode scan = new LogicalRelScan( cluster, traits, table );
        return LogicalRelProject.create( scan, , );*/
    }


    @Override
    public AlgDataType lockOutputType( List<AlgDataType> inTypes, Map<String, SettingValue> settings ) throws Exception {
        lockedEntity = getEntity( settings.get( TABLE_KEY ) );
        return getOutputType( lockedEntity );
    }


    @Override
    public void pipe( List<InputPipe> inputs, OutputPipe output, Map<String, SettingValue> settings, PipeExecutionContext ctx ) throws Exception {
        // TODO: Create new transaction or use ctx?
        throw new NotImplementedException();
    }


    private LogicalTable getEntity( SettingValue setting ) throws ActivityException {
        EntityValue entitySetting = (EntityValue) setting;
        return Catalog.snapshot().rel().getTable( entitySetting.getNamespace(), entitySetting.getName() ).orElseThrow(
                () -> new InvalidSettingException( "Specified table does not exist", "table" ) );
    }


    private AlgDataType getOutputType( LogicalTable table ) {
        // we insert the primary key column
        AlgDataTypeFactory factory = AlgDataTypeFactory.DEFAULT;
        List<Long> ids = new ArrayList<>();
        List<AlgDataType> types = new ArrayList<>();
        List<String> names = new ArrayList<>();

        ids.add( null );
        types.add( factory.createPolyType( PolyType.BIGINT ) );
        names.add( StorageManager.PK_COL );

        for ( AlgDataTypeField field : table.getTupleType().getFields() ) {
            ids.add( null );
            types.add( field.getType() );
            names.add( field.getName() );
        }

        return factory.createStructType( ids, types, names );
    }

}
