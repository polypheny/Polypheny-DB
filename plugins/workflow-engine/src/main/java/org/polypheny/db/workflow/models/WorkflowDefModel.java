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

import com.fasterxml.jackson.annotation.JsonIgnore;
import java.time.Instant;
import java.util.Date;
import java.util.HashMap;
import java.util.Map;
import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.Setter;


@Getter
@AllArgsConstructor
public class WorkflowDefModel {

    @Setter
    private String name;
    @Setter
    private String group; // null or '' for default group

    private final Map<Integer, VersionInfo> versions;


    public WorkflowDefModel( String name, String group ) {
        this.name = name;
        this.group = group;
        versions = new HashMap<>();
    }


    public int addVersion( String description ) {
        int version = getHighestVersion() + 1; // increment version
        versions.put( version, new VersionInfo( description, Date.from( Instant.now() ) ) );
        return version;
    }


    public void removeVersion( int version ) {
        versions.remove( version );
    }


    @JsonIgnore
    public int getHighestVersion() {
        return versions.keySet().stream().max( Integer::compareTo ).orElse( -1 );
    }


    @JsonIgnore
    public boolean isEmpty() {
        return versions.isEmpty();
    }


    @Override
    public String toString() {
        return "WorkflowDefModel{" +
                "name='" + name + "'" +
                ", " + versions.size() + " version(s)" + // Displaying the number of versions, not the entire map
                ", highestVersion=" + getHighestVersion() +
                '}';
    }


    @Getter
    @AllArgsConstructor
    public static class VersionInfo {

        private String description;
        private Date creationTime;


        @Override
        public String toString() {
            return "VersionInfo{" +
                    "description='" + description + '\'' +
                    ", creationTime=" + creationTime +
                    '}';
        }

    }

}
