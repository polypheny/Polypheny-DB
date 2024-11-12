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

package org.polypheny.db.workflow.models;

import java.util.UUID;
import lombok.AllArgsConstructor;
import lombok.Value;

@Value
@AllArgsConstructor
public class SessionModel {

    SessionModelType type;
    UUID sessionId; // TODO: remove redundant session id, since it's already the map key? Or send list

    // USER_SESSION fields:
    UUID workflowId;
    Integer version;


    public SessionModel( SessionModelType type, UUID sId ) {
        this.type = type;
        this.sessionId = sId;
        this.workflowId = null;
        this.version = null;

    }


    public enum SessionModelType {
        USER_SESSION,
        API_SESSION,
        JOB_SESSION
    }

}
