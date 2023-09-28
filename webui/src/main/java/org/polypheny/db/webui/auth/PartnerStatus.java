/*
 * Copyright 2019-2023 The Polypheny Project
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

package org.polypheny.db.webui.auth;

import java.util.UUID;
import lombok.Getter;
import org.eclipse.jetty.websocket.api.Session;

@Getter
public class PartnerStatus {

    public final UUID id;
    private final Session session;


    public PartnerStatus( UUID id, Session session ) {
        this.id = id;
        this.session = session;
    }


    public PartnerStatus( Session session ) {
        this( UUID.randomUUID(), session );
    }

}
