/*
 * Copyright 2019-2025 The Polypheny Project
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

package org.polypheny.db.workflow.engine.execution.pipe;

import java.util.List;
import org.polypheny.db.algebra.type.AlgDataType;
import org.polypheny.db.type.entity.PolyValue;
import org.polypheny.db.workflow.dag.activities.Pipeable.PipeInterruptedException;
import org.polypheny.db.workflow.engine.execution.context.PipeExecutionContext;
import org.polypheny.db.workflow.engine.storage.writer.CheckpointWriter;

public class CheckpointOutputPipe implements OutputPipe {

    private final AlgDataType type;
    private final CheckpointWriter writer;
    private final PipeExecutionContext ctx;
    private final long totalCount;
    private final long countDelta;
    private final boolean canEstimateProgress;
    private long count;


    public CheckpointOutputPipe( AlgDataType type, CheckpointWriter writer, PipeExecutionContext sourceCtx, long estimatedTotalCount ) {
        this.type = type;
        this.writer = writer;

        this.totalCount = estimatedTotalCount;
        this.canEstimateProgress = totalCount > 0;
        this.countDelta = Math.max( totalCount / 100, 1 );
        this.ctx = sourceCtx;
    }


    @Override
    public boolean put( List<PolyValue> value ) throws PipeInterruptedException {
        ctx.checkPipeInterrupted();
        writer.write( value );
        count++;
        if ( canEstimateProgress && count % countDelta == 0 ) {
            ctx.updateProgress( getEstimatedProgress() );
        }
        return true; // no limit to the number of tuples that can be written
    }


    @Override
    public double getEstimatedProgress() {
        return canEstimateProgress ? (double) count / totalCount : -1;
    }


    @Override
    public void close() {
        try {
            writer.close();
        } catch ( Exception ignored ) {

        }
    }


    @Override
    public AlgDataType getType() {
        return type;
    }

}
