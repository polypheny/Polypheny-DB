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

import java.util.List;
import java.util.Optional;
import org.polypheny.db.algebra.AlgNode;
import org.polypheny.db.algebra.logical.relational.LogicalRelProject;
import org.polypheny.db.algebra.type.AlgDataType;
import org.polypheny.db.catalog.exceptions.GenericRuntimeException;
import org.polypheny.db.plan.AlgCluster;
import org.polypheny.db.type.entity.PolyValue;
import org.polypheny.db.workflow.dag.activities.Activity;
import org.polypheny.db.workflow.dag.activities.Activity.ActivityCategory;
import org.polypheny.db.workflow.dag.activities.Activity.PortType;
import org.polypheny.db.workflow.dag.activities.ActivityException;
import org.polypheny.db.workflow.dag.activities.Fusable;
import org.polypheny.db.workflow.dag.activities.Pipeable;
import org.polypheny.db.workflow.dag.activities.TypePreview;
import org.polypheny.db.workflow.dag.annotations.ActivityDefinition;
import org.polypheny.db.workflow.dag.annotations.ActivityDefinition.InPort;
import org.polypheny.db.workflow.dag.annotations.ActivityDefinition.OutPort;
import org.polypheny.db.workflow.dag.annotations.BoolSetting;
import org.polypheny.db.workflow.dag.annotations.IntSetting;
import org.polypheny.db.workflow.dag.settings.BoolValue;
import org.polypheny.db.workflow.dag.settings.IntValue;
import org.polypheny.db.workflow.dag.settings.SettingDef.Settings;
import org.polypheny.db.workflow.dag.settings.SettingDef.SettingsPreview;
import org.polypheny.db.workflow.engine.execution.context.ExecutionContext;
import org.polypheny.db.workflow.engine.execution.context.FuseExecutionContext;
import org.polypheny.db.workflow.engine.execution.context.PipeExecutionContext;
import org.polypheny.db.workflow.engine.execution.pipe.InputPipe;
import org.polypheny.db.workflow.engine.execution.pipe.OutputPipe;
import org.polypheny.db.workflow.engine.storage.reader.CheckpointReader;
import org.polypheny.db.workflow.engine.storage.reader.RelReader;
import org.polypheny.db.workflow.engine.storage.writer.RelWriter;

@ActivityDefinition(type = "debug", displayName = "Relational Identity Activity with Debugging",
        categories = { ActivityCategory.TRANSFORM, ActivityCategory.RELATIONAL },
        inPorts = { @InPort(type = PortType.REL) },
        outPorts = { @OutPort(type = PortType.REL) }
)

@IntSetting(key = "delay", displayName = "Delay (ms)", defaultValue = 50, min = 0)
@IntSetting(key = "pipeDelay", displayName = "Tuple-wise Delay for Pipelining (ms)", defaultValue = 1, min = 0)
@BoolSetting(key = "canPipe", displayName = "Enable Pipelining", defaultValue = false)
@BoolSetting(key = "canFuse", displayName = "Enable Fusion", defaultValue = false)
@BoolSetting(key = "isSuccessful", displayName = "Successful Execution", defaultValue = true)

@SuppressWarnings("unused")
public class DebugActivity implements Activity, Pipeable, Fusable {

    @Override
    public List<TypePreview> previewOutTypes( List<TypePreview> inTypes, SettingsPreview settings ) throws ActivityException {
        return inTypes.get( 0 ).asOutTypes();
    }


    @Override
    public void execute( List<CheckpointReader> inputs, Settings settings, ExecutionContext ctx ) throws Exception {
        RelReader input = (RelReader) inputs.get( 0 );
        RelWriter output = ctx.createRelWriter( 0, input.getTupleType() );

        int delay = settings.get( "delay", IntValue.class ).getValue();
        if ( delay > 1000 ) {
            int delta = 100; // delta ms per step
            int steps = delay / delta;
            for ( int i = 0; i < steps; i++ ) {
                Thread.sleep( delta );
                ctx.checkInterrupted();
            }
        } else {
            Thread.sleep( delay );
        }
        checkFail( settings );
        output.write( input.getIterator() );
    }


    @Override
    public Optional<Boolean> canFuse( List<TypePreview> inTypes, SettingsPreview settings ) {
        return settings.get( "canFuse", BoolValue.class ).map( BoolValue::getValue );
    }


    @Override
    public AlgNode fuse( List<AlgNode> inputs, Settings settings, AlgCluster cluster, FuseExecutionContext ctx ) throws Exception {
        checkFail( settings );
        return LogicalRelProject.identity( inputs.get( 0 ) );
    }


    @Override
    public Optional<Boolean> canPipe( List<TypePreview> inTypes, SettingsPreview settings ) {
        return settings.get( "canPipe", BoolValue.class ).map( BoolValue::getValue );
    }


    @Override
    public AlgDataType lockOutputType( List<AlgDataType> inTypes, Settings settings ) throws Exception {
        return inTypes.get( 0 );
    }


    @Override
    public void pipe( List<InputPipe> inputs, OutputPipe output, Settings settings, PipeExecutionContext ctx ) throws Exception {
        int millis = settings.get( "pipeDelay", IntValue.class ).getValue();
        for ( List<PolyValue> value : inputs.get( 0 ) ) {
            try {
                Thread.sleep( millis );
                checkFail( settings );
            } catch ( InterruptedException e ) {
                throw new PipeInterruptedException( e );
            }
            if ( !output.put( value ) ) {
                finish( inputs );
                return;
            }
        }
    }


    private void checkFail( Settings settings ) {
        if ( !settings.get( "isSuccessful", BoolValue.class ).getValue() ) {
            throw new GenericRuntimeException( "Debug activity was configured to fail." );
        }
    }

}
