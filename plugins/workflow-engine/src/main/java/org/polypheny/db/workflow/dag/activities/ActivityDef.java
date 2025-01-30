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
import com.fasterxml.jackson.annotation.JsonProperty;
import java.lang.annotation.Annotation;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import lombok.AccessLevel;
import lombok.Getter;
import lombok.Value;
import org.polypheny.db.workflow.dag.activities.Activity.ActivityCategory;
import org.polypheny.db.workflow.dag.activities.Activity.PortType;
import org.polypheny.db.workflow.dag.annotations.ActivityDefinition;
import org.polypheny.db.workflow.dag.annotations.ActivityDefinition.InPort;
import org.polypheny.db.workflow.dag.annotations.ActivityDefinition.OutPort;
import org.polypheny.db.workflow.dag.annotations.AdvancedGroup;
import org.polypheny.db.workflow.dag.annotations.DefaultGroup;
import org.polypheny.db.workflow.dag.edges.DataEdge;
import org.polypheny.db.workflow.dag.edges.Edge;
import org.polypheny.db.workflow.dag.settings.GroupDef;
import org.polypheny.db.workflow.dag.settings.SettingDef;

@Value
public class ActivityDef {

    @JsonIgnore
    Class<? extends Activity> activityClass;
    String type;
    String displayName;
    String shortDescription;
    String longDescription;
    ActivityCategory[] categories;
    InPortDef[] inPorts;
    OutPortDef[] outPorts;
    String iconPath;
    List<GroupDef> groups;
    Map<String, SettingDef> settings;
    boolean fusable;
    boolean pipeable;
    boolean variableWriter;


    public static ActivityDef fromAnnotations( Class<? extends Activity> activityClass,
            ActivityDefinition def, Annotation groups, DefaultGroup defaultGroup, AdvancedGroup advancedGroup, Annotation[] allAnnotations,
            boolean isFusable, boolean isPipeable, boolean isVariableWriter) {
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
                def.shortDescription(),
                def.longDescription().isEmpty() ? def.shortDescription() : def.longDescription(),
                def.categories(),
                InPortDef.fromAnnotations( def.inPorts() ),
                OutPortDef.fromAnnotations( def.outPorts() ),
                def.iconPath(),
                GroupDef.fromAnnotation( groups, defaultGroup, advancedGroup ),
                settings,
                isFusable,
                isPipeable,
                isVariableWriter
        );
    }


    @JsonIgnore
    public boolean hasMultiInPort() {
        return inPorts.length > 0 && inPorts[inPorts.length - 1].isMulti;
    }


    @JsonIgnore
    public int getDynamicInPortCount( Collection<Edge> inEdges ) {
        int count = (int) inEdges.stream().filter(
                e -> e instanceof DataEdge d && d.isMulti()
        ).count();
        return inPorts.length + Math.max( 0, count - 1 ); // - 1 to avoid counting first multi twice
    }


    /**
     * This method is preferred to {@code getInPorts()[index]},
     * as it takes multi-ports into account.
     *
     * @param index The index of the connection. If the activity has a multi-port, the index can be arbitrarily large.
     * @return the InPortDef for the port corresponding to the index
     */
    @JsonIgnore
    public InPortDef getInPort( int index ) {
        if ( index >= inPorts.length ) {
            InPortDef port = inPorts[inPorts.length - 1];
            if ( !port.isMulti() ) {
                throw new IndexOutOfBoundsException();
            }
            return port;
        }
        return inPorts[index];
    }


    /**
     * This method is preferred to {@code getInPortTypes()[index]},
     * as it takes multi-ports into account.
     *
     * @param index The index of the connection. If the activity has a multi-port, the index can be arbitrarily large
     * @return the PortType for the InPort corresponding to the index.
     */
    @JsonIgnore
    public PortType getInPortType( int index ) {
        return getInPort( index ).getType();
    }


    @JsonIgnore
    public PortType getOutPortType( int index ) {
        return outPorts[index].getType();
    }


    @JsonIgnore
    public PortType[] getOutPortTypes() {
        return Arrays.stream( outPorts ).map( OutPortDef::getType ).toArray( PortType[]::new );
    }


    @JsonIgnore
    public Set<Integer> getRequiredInPorts() {
        Set<Integer> set = new HashSet<>();
        for ( int i = 0; i < inPorts.length; i++ ) {
            if ( !inPorts[i].isOptional ) {
                set.add( i );
            }
        }
        return set;
    }


    public boolean hasCategory( ActivityCategory category ) {
        if ( category == null ) {
            return false;
        }
        for ( ActivityCategory cat : categories ) {
            if ( category.equals( cat ) ) {
                return true;
            }
        }
        return false;
    }


    @Value
    public static class InPortDef {

        PortType type;
        String description;

        @Getter(AccessLevel.NONE)
        boolean isOptional;
        @Getter(AccessLevel.NONE)
        boolean isMulti;


        private InPortDef( InPort inPort ) {
            type = inPort.type();
            description = inPort.description();
            isOptional = inPort.isOptional();
            isMulti = inPort.isMulti();
        }


        @JsonProperty("isOptional")
        public boolean isOptional() {
            return isOptional;
        }


        @JsonProperty("isMulti")
        public boolean isMulti() {
            return isMulti;
        }


        public static InPortDef[] fromAnnotations( InPort[] inPorts ) {
            InPortDef[] defs = Arrays.stream( inPorts ).map( InPortDef::new ).toArray( InPortDef[]::new );
            for ( int i = 0; i < defs.length - 1; i++ ) {
                assert !defs[i].isMulti : "Only the last InPort can set isMulti to true";
            }
            return defs;
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
