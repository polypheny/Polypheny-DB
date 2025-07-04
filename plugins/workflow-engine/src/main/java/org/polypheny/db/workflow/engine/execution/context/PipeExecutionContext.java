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

package org.polypheny.db.workflow.engine.execution.context;

import java.util.List;
import org.polypheny.db.transaction.Transaction;
import org.polypheny.db.workflow.dag.activities.Activity;
import org.polypheny.db.workflow.dag.activities.Pipeable.PipeInterruptedException;
import org.polypheny.db.workflow.dag.variables.ReadableVariableStore;

public interface PipeExecutionContext {

    /**
     * Checks whether pipe execution was interrupted.
     * This function generally does not have to be called manually, as the interrupt check is performed while putting a value in the output pipe.
     * If the activity is a pure tuple consumer, then it is recommended to periodically call it.
     *
     * @throws PipeInterruptedException if execution was interrupted
     */
    void checkPipeInterrupted() throws PipeInterruptedException;

    /**
     * Manually update the progress of this activity.
     * It should only be used by activities that have no output and thus only perform side effects.
     * Any activity that pipes its tuples to an output pipe must not call this method, as the progress is updated
     * automatically by the pipe.
     *
     * @param value the updated progress value
     */
    void updateProgress( double value );

    /**
     * Returns a transaction to be used for extracting or loading data from data stores or data sources.
     * The transaction MUST NOT be committed or rolled back, as this is done externally.
     * This method can only be called by {@link Activity.ActivityCategory#EXTRACT} or {@link Activity.ActivityCategory#LOAD} activities.
     *
     * @return A transaction to be used for either reading or writing arbitrary entities in this Polypheny instance.
     */
    Transaction getTransaction();

    ReadableVariableStore getVariableStore();

    /**
     * Logs an informational message.
     *
     * <p>Note: Logs are saved to a circular queue limited to the log capacity specified in the workflow config.
     * Use logging sparingly to avoid dropping messages.</p>
     *
     * @param message the informational message to log
     */
    void logInfo( String message );

    /**
     * Logs a warning message.
     *
     * <p>Note: Logs are saved to a circular queue limited to the log capacity specified in the workflow config.
     * Use logging sparingly to avoid dropping messages.</p>
     *
     * @param message the warning message to log
     */
    void logWarning( String message );

    /**
     * Logs a non-catastrophic error message.
     * If the error makes it impossible for the activity to successfully execute, it is better throw an exception.
     *
     * <p>Note: Logs are saved to a circular queue limited to the log capacity specified in the workflow config.
     * Use logging sparingly to avoid dropping messages.</p>
     *
     * @param message the error message to log
     */
    void logError( String message );

    List<Long> getEstimatedInCounts();

}
