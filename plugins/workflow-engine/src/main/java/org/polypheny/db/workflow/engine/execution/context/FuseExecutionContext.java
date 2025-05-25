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

public interface FuseExecutionContext {


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

}
