/*
 * Copyright 2019-2022 The Polypheny Project
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

package org.polypheny.db.adaptiveness.models;


public class PolicyChangeRequest {

    public String requestType;
    public String clauseName;
    public String targetName;
    public boolean booleanValue;
    public int numberValue;

    // has to be Long because sometimes it is not needed and therefore null
    public Long targetId;


    public PolicyChangeRequest( String requestType, String clauseName, String targetName, boolean booleanValue, Long targetId ) {
        this.requestType = requestType;
        this.clauseName = clauseName;
        this.targetName = targetName;
        this.booleanValue = booleanValue;
        this.targetId = targetId;
    }

    public PolicyChangeRequest( String requestType, String clauseName, String targetName, int numberValue, Long targetId ) {
        this.requestType = requestType;
        this.clauseName = clauseName;
        this.targetName = targetName;
        this.numberValue = numberValue;
        this.targetId = targetId;
    }

}
