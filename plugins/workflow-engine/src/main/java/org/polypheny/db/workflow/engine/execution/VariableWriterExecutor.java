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

package org.polypheny.db.workflow.engine.execution;

import java.util.List;
import java.util.UUID;
import org.polypheny.db.workflow.dag.Workflow;
import org.polypheny.db.workflow.dag.activities.ActivityWrapper;
import org.polypheny.db.workflow.dag.activities.VariableWriter;
import org.polypheny.db.workflow.dag.settings.SettingDef.Settings;
import org.polypheny.db.workflow.engine.execution.context.ExecutionContextImpl;
import org.polypheny.db.workflow.engine.monitoring.ExecutionInfo;
import org.polypheny.db.workflow.engine.storage.StorageManager;
import org.polypheny.db.workflow.engine.storage.reader.CheckpointReader;

public class VariableWriterExecutor extends Executor {

    private final ActivityWrapper wrapper;
    private ExecutionContextImpl ctx;


    public VariableWriterExecutor( StorageManager sm, Workflow workflow, UUID activityId, ExecutionInfo info ) {
        super( sm, workflow, info );
        this.wrapper = workflow.getActivity( activityId );
    }


    @Override
    void execute() throws ExecutorException {
        List<CheckpointReader> inputs = getReaders( wrapper );

        try ( CloseableList ignored = new CloseableList( inputs );
                ExecutionContextImpl ctx = new ExecutionContextImpl( wrapper, sm, info ) ) {
            this.ctx = ctx;
            VariableWriter activity = (VariableWriter) wrapper.getActivity();
            Settings settings = wrapper.resolveSettings();
            ctx.logInfo( "Starting variable writer execution with settings: " + settings.serialize() );
            activity.execute( inputs, settings, ctx, wrapper.getVariables() );
            wrapper.setOutTypePreview( sm.getCheckpointPreviewTypes( wrapper.getId() ) );
        } catch ( Exception e ) {
            e.printStackTrace();
            throw new ExecutorException( e, wrapper.getId() );
        }
    }


    @Override
    public ExecutorType getType() {
        return ExecutorType.VARIABLE_WRITER;
    }


    @Override
    public void interrupt() {
        super.interrupt();
        if ( ctx != null ) {
            ctx.setInterrupted();
        }

    }

}
