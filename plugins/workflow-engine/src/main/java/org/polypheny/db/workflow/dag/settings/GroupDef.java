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

package org.polypheny.db.workflow.dag.settings;

import java.lang.annotation.Annotation;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import lombok.Value;
import org.polypheny.db.workflow.dag.annotations.AdvancedGroup;
import org.polypheny.db.workflow.dag.annotations.DefaultGroup;
import org.polypheny.db.workflow.dag.annotations.Group;
import org.polypheny.db.workflow.dag.annotations.Group.Subgroup;

@Value
public class GroupDef {

    public static String DEFAULT_GROUP = "";
    public static String ADVANCED_GROUP = "advanced";
    public static String DEFAULT_SUBGROUP = "";

    private static String DEFAULT_GROUP_NAME = "Settings";
    private static String ADVANCED_GROUP_NAME = "Advanced";

    String key;
    String displayName;
    int position;
    SubgroupDef[] subgroups;


    private static GroupDef fromAnnotation( Group a ) {
        assert !a.key().equals( GroupDef.DEFAULT_GROUP ) : "Cannot redefine predefined DEFAULT subgroup.";
        assert !a.key().equals( GroupDef.ADVANCED_GROUP ) : "Cannot redefine predefined ADVANCED subgroup.";

        return new GroupDef(
                a.key(),
                a.displayName(),
                a.pos(),
                SubgroupDef.fromAnnotations( a.subgroups() )
        );
    }


    private static List<GroupDef> fromAnnotation( Group.List annotations ) {
        List<GroupDef> groups = new ArrayList<>();
        for ( Group annotation : annotations.value() ) {
            groups.add( fromAnnotation( annotation ) );
        }
        return groups;
    }


    private static GroupDef getDefaultGroup( DefaultGroup defaultGroup ) {
        Subgroup[] subgroups = defaultGroup == null ? new Subgroup[0] : defaultGroup.subgroups();
        return new GroupDef( GroupDef.DEFAULT_GROUP, DEFAULT_GROUP_NAME, Integer.MIN_VALUE, SubgroupDef.fromAnnotations( subgroups ) );
    }


    private static GroupDef getAdvancedGroup( AdvancedGroup advancedGroup ) {
        Subgroup[] subgroups = advancedGroup == null ? new Subgroup[0] : advancedGroup.subgroups();
        return new GroupDef( GroupDef.ADVANCED_GROUP, ADVANCED_GROUP_NAME, Integer.MAX_VALUE, SubgroupDef.fromAnnotations( subgroups ) );
    }


    public static List<GroupDef> fromAnnotation( Annotation annotation, DefaultGroup defaultGroup, AdvancedGroup advancedGroup ) {
        List<GroupDef> groups = new ArrayList<>();
        groups.add( GroupDef.getDefaultGroup( defaultGroup ) );
        if ( annotation != null ) {
            if ( annotation instanceof Group.List a ) {
                groups.addAll( fromAnnotation( a ) );
            } else if ( annotation instanceof Group a ) {
                groups.add( fromAnnotation( a ) );
            }
        }
        groups.add( GroupDef.getAdvancedGroup( advancedGroup ) );
        return groups;
    }


    @Value
    public static class SubgroupDef {

        String key;
        String displayName;
        int position;


        private SubgroupDef( Subgroup a ) {
            key = a.key();
            displayName = a.displayName();
            position = a.pos();
        }


        public static SubgroupDef[] fromAnnotations( Subgroup[] subgroups ) {
            return Arrays.stream( subgroups ).map( SubgroupDef::new ).toArray( SubgroupDef[]::new );
        }

    }

}
