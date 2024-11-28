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

package org.polypheny.db.workflow.engine.execution.context;

import org.polypheny.db.transaction.Transaction;
import org.polypheny.db.workflow.dag.activities.Activity;

public interface PipeExecutionContext {

    // no checkInterrupted(), since we use actual Interrupts. This is required to be able to interrupt
    // activities blocked by a pipe.

    void updateProgress( double value );

    /**
     * Returns a transaction to be used for extracting or loading data from data stores or data sources.
     * The transaction MUST NOT be committed or rolled back, as this is done externally.
     * This method can only be called by {@link Activity.ActivityCategory#EXTRACT} or {@link Activity.ActivityCategory#LOAD} activities.
     *
     * @return A transaction to be used for either reading or writing arbitrary entities in this Polypheny instance.
     */
    Transaction getTransaction();

}
