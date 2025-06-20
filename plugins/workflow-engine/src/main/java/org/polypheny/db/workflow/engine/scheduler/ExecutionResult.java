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

package org.polypheny.db.workflow.engine.scheduler;

import lombok.AllArgsConstructor;
import lombok.Value;
import lombok.experimental.Delegate;
import org.polypheny.db.workflow.engine.execution.Executor;
import org.polypheny.db.workflow.engine.execution.Executor.ExecutorException;

@Value
@AllArgsConstructor
public class ExecutionResult {

    @Delegate(excludes = Exclude.class)
    ExecutionSubmission submission;
    ExecutorException exception;


    public ExecutionResult( ExecutionSubmission submission ) {
        this.submission = submission;
        exception = null;
        submission.getInfo().setSuccess( isSuccess() );
    }


    public boolean isSuccess() {
        return exception == null;
    }


    private interface Exclude {

        Executor getExecutor();

    }

}
