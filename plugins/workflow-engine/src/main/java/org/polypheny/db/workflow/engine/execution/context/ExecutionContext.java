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

import org.polypheny.db.algebra.type.AlgDataType;
import org.polypheny.db.transaction.Transaction;
import org.polypheny.db.workflow.dag.activities.Activity;
import org.polypheny.db.workflow.dag.variables.ReadableVariableStore;
import org.polypheny.db.workflow.engine.execution.Executor.ExecutorException;
import org.polypheny.db.workflow.engine.storage.writer.CheckpointWriter;
import org.polypheny.db.workflow.engine.storage.writer.DocWriter;
import org.polypheny.db.workflow.engine.storage.writer.LpgWriter;
import org.polypheny.db.workflow.engine.storage.writer.RelWriter;

public interface ExecutionContext {

    void checkInterrupted() throws ExecutorException;

    void updateProgress( double value );

    /**
     * Creates a {@link RelWriter} for the specified output index with the given tuple type.
     * The writer is automatically closed by the executor that executes the activity.
     *
     * @param idx the output index.
     * @param tupleType the schema of the output.
     * @return a {@link RelWriter} for writing data to the output.
     */
    RelWriter createRelWriter( int idx, AlgDataType tupleType );

    /**
     * Creates a {@link DocWriter} for the specified output index.
     * The writer is automatically closed by the executor that executes the activity.
     *
     * @param idx the output index.
     * @return a {@link DocWriter} for writing data to the output.
     */
    DocWriter createDocWriter( int idx );

    /**
     * Creates a {@link LpgWriter} for the specified output index.
     * The writer is automatically closed by the executor that executes the activity.
     *
     * @param idx the output index.
     * @return a {@link LpgWriter} for writing data to the output.
     */
    LpgWriter createLpgWriter( int idx );

    /**
     * Creates a CheckpointWriter for the specified output index and tuple type.
     * The data model of the checkpoint is automatically inferred from the output port definition.
     * In case of {@code PortType.ANY}, the relational data model is used.
     * The writer is automatically closed by the executor that executes the activity.
     *
     * @param idx the output index.
     * @param tupleType the schema of the output. Only relevant for relational outputs.
     * @return a CheckpointWriter for writing data to the output.
     */
    CheckpointWriter createWriter( int idx, AlgDataType tupleType );

    boolean writerWasCreated( int idx );

    /**
     * Returns a transaction to be used for extracting or loading data from data stores or data sources or executing fused activities.
     * The transaction MUST NOT be committed or rolled back, as this is done externally.
     * This method should only be called by {@link Activity.ActivityCategory#EXTRACT}, {@link Activity.ActivityCategory#LOAD} or fusable activities.
     *
     * @return A transaction to be used for either reading or writing arbitrary entities in this Polypheny instance or executing fused activities.
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

    void throwException( String message ) throws ExecutorException;

    void throwException( Throwable cause ) throws ExecutorException;

    boolean isLogErrors();

}
