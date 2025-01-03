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

package org.polypheny.db.workflow.session;

import java.util.UUID;
import org.apache.commons.lang3.NotImplementedException;
import org.polypheny.db.workflow.dag.Workflow;
import org.polypheny.db.workflow.models.SessionModel;
import org.polypheny.db.workflow.models.SessionModel.SessionModelType;

public class ApiSession extends AbstractSession {


    public ApiSession( UUID sessionId, Workflow wf ) {
        super( wf, sessionId );
    }


    @Override
    public void terminate() {
        throw new NotImplementedException();
    }


    @Override
    public SessionModel toModel() {
        return new SessionModel( SessionModelType.API_SESSION, sessionId, getSubscriberCount() );
    }

}