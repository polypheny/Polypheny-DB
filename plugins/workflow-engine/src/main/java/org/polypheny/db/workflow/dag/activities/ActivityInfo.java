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

package org.polypheny.db.workflow.dag.activities;

import com.fasterxml.jackson.annotation.JsonIgnore;
import java.util.Arrays;
import lombok.Value;
import org.polypheny.db.workflow.dag.activities.Activity.ActivityCategory;
import org.polypheny.db.workflow.dag.activities.Activity.PortType;
import org.polypheny.db.workflow.dag.annotations.ActivityDefinition;
import org.polypheny.db.workflow.dag.annotations.ActivityDefinition.InPort;
import org.polypheny.db.workflow.dag.annotations.ActivityDefinition.OutPort;

@Value
public class ActivityInfo {

    @JsonIgnore
    Class<? extends Activity> activityClass;
    String type;
    String displayName;
    String description;
    ActivityCategory[] categories;
    InPortInfo[] inPorts;
    OutPortInfo[] outPorts;
    String iconPath;


    public static ActivityInfo fromAnnotations(Class<? extends Activity> activityClass, ActivityDefinition def ) {
        return new ActivityInfo(
                activityClass,
                def.type(),
                def.displayName(),
                def.description(),
                def.categories(),
                InPortInfo.fromAnnotations( def.inPorts() ),
                OutPortInfo.fromAnnotations( def.outPorts() ),
                def.iconPath() );
    }


    @Value
    public static class InPortInfo {

        PortType type;
        String description;
        boolean isOptional;


        private InPortInfo( InPort inPort ) {
            type = inPort.type();
            description = inPort.description();
            isOptional = inPort.isOptional();
        }


        public static InPortInfo[] fromAnnotations( InPort[] inPorts ) {
            return Arrays.stream( inPorts ).map( InPortInfo::new ).toArray( InPortInfo[]::new );
        }

    }


    @Value
    public static class OutPortInfo {

        PortType type;
        String description;


        private OutPortInfo( OutPort outPort ) {
            type = outPort.type();
            description = outPort.description();
        }


        public static OutPortInfo[] fromAnnotations( OutPort[] outPorts ) {
            return Arrays.stream( outPorts ).map( OutPortInfo::new ).toArray( OutPortInfo[]::new );
        }

    }

}
