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
import org.polypheny.db.workflow.dag.settings.SettingDef.Settings;
import org.polypheny.db.workflow.engine.execution.context.ExecutionContextImpl;
import org.polypheny.db.workflow.engine.monitoring.ExecutionInfo;
import org.polypheny.db.workflow.engine.storage.StorageManager;
import org.polypheny.db.workflow.engine.storage.reader.CheckpointReader;

/**
 * Executor that executes a single activity using its execute() method.
 * This executor should only be used if the activity is neither fusable, pipeable, nor is it a VariableWriter.
 */
public class DefaultExecutor extends Executor {

    private final ActivityWrapper wrapper;
    private ExecutionContextImpl ctx;


    public DefaultExecutor( StorageManager sm, Workflow wf, UUID activityId, ExecutionInfo info ) {
        super( sm, wf, info );
        this.wrapper = wf.getActivity( activityId );
    }


    @Override
    void execute() throws ExecutorException {
        List<CheckpointReader> inputs = getReaders( wrapper );

        try ( CloseableList ignored = new CloseableList( inputs );
                ExecutionContextImpl ctx = new ExecutionContextImpl( wrapper, sm, info ) ) {
            this.ctx = ctx;
            Settings settings = wrapper.resolveSettings();
            ctx.logInfo( "Starting execution with settings: " + settings.serialize() );
            wrapper.getActivity().execute( inputs, settings, ctx );
            wrapper.setOutTypePreview( sm.getCheckpointPreviewTypes( wrapper.getId() ) );
            ctx.updateProgress( 1 ); // ensure progress is correct
        } catch ( Exception e ) {
            e.printStackTrace();
            throw new ExecutorException( e, wrapper.getId() );
        }
    }


    @Override
    public ExecutorType getType() {
        return ExecutorType.DEFAULT;
    }


    @Override
    public void interrupt() {
        super.interrupt();
        if ( ctx != null ) {
            ctx.setInterrupted();
        }
    }


}
