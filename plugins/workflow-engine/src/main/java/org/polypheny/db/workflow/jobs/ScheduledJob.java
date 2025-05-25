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

package org.polypheny.db.workflow.jobs;

import org.polypheny.db.workflow.jobs.JobManager.WorkflowJobException;
import org.polypheny.db.workflow.models.JobModel;

public class ScheduledJob extends JobTrigger {

    private final CronScheduler scheduler = CronScheduler.getInstance();
    String schedule;


    public ScheduledJob( JobModel model ) throws WorkflowJobException {
        super( model.getJobId(), TriggerType.SCHEDULED, model.getWorkflowId(), model.getVersion(),
                model.isEnableOnStartup(), model.getName(), model.getMaxRetries(), model.isPerformance(), model.getVariables() );
        if ( model.getType() != TriggerType.SCHEDULED ) {
            throw new IllegalArgumentException( "JobModel must be of type SCHEDULED" );
        }
        this.schedule = model.getSchedule();
        scheduler.validateCronExpression( this.schedule );
    }


    @Override
    public void onEnable() {
        scheduler.addJob( jobId, schedule, () -> trigger( "Scheduled execution" ) );
    }


    @Override
    public void onDisable() {
        scheduler.removeJob( jobId );
    }


    @Override
    public JobModel toModel() {
        return new JobModel( jobId, type, workfowId, version, enableOnStartup, name, maxRetries, performance, variables, schedule );
    }

}
