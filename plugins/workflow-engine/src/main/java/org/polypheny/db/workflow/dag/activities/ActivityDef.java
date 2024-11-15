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
import java.lang.annotation.Annotation;
import java.util.Arrays;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import lombok.Value;
import org.polypheny.db.workflow.dag.activities.Activity.ActivityCategory;
import org.polypheny.db.workflow.dag.activities.Activity.PortType;
import org.polypheny.db.workflow.dag.annotations.ActivityDefinition;
import org.polypheny.db.workflow.dag.annotations.ActivityDefinition.InPort;
import org.polypheny.db.workflow.dag.annotations.ActivityDefinition.OutPort;
import org.polypheny.db.workflow.dag.annotations.AdvancedGroup;
import org.polypheny.db.workflow.dag.annotations.DefaultGroup;
import org.polypheny.db.workflow.dag.settings.GroupDef;
import org.polypheny.db.workflow.dag.settings.SettingDef;

@Value
public class ActivityDef {

    @JsonIgnore
    Class<? extends Activity> activityClass;
    String type;
    String displayName;
    String description;
    ActivityCategory[] categories;
    InPortDef[] inPorts;
    OutPortDef[] outPorts;
    String iconPath;
    List<GroupDef> groups;
    Map<String, SettingDef> settings;


    public static ActivityDef fromAnnotations( Class<? extends Activity> activityClass, ActivityDefinition def, Annotation groups, DefaultGroup defaultGroup, AdvancedGroup advancedGroup, Annotation[] allAnnotations ) {
        Map<String, SettingDef> settings = new LinkedHashMap<>(); // This ensures that the order of Settings of the same type is preserved, which is useful for the UI.
        for ( SettingDef setting : SettingDef.fromAnnotations( allAnnotations ) ) {
            String key = setting.getKey();
            assert !settings.containsKey( key ) : activityClass.getSimpleName() + "has Setting with duplicate key: " + key;
            settings.put( key, setting );
        }
        return new ActivityDef(
                activityClass,
                def.type(),
                def.displayName(),
                def.description(),
                def.categories(),
                InPortDef.fromAnnotations( def.inPorts() ),
                OutPortDef.fromAnnotations( def.outPorts() ),
                def.iconPath(),
                GroupDef.fromAnnotation( groups, defaultGroup, advancedGroup ),
                settings
        );
    }


    @Value
    public static class InPortDef {

        PortType type;
        String description;
        boolean isOptional;


        private InPortDef( InPort inPort ) {
            type = inPort.type();
            description = inPort.description();
            isOptional = inPort.isOptional();
        }


        public static InPortDef[] fromAnnotations( InPort[] inPorts ) {
            return Arrays.stream( inPorts ).map( InPortDef::new ).toArray( InPortDef[]::new );
        }

    }


    @Value
    public static class OutPortDef {

        PortType type;
        String description;


        private OutPortDef( OutPort outPort ) {
            type = outPort.type();
            description = outPort.description();
        }


        public static OutPortDef[] fromAnnotations( OutPort[] outPorts ) {
            return Arrays.stream( outPorts ).map( OutPortDef::new ).toArray( OutPortDef[]::new );
        }

    }

}
