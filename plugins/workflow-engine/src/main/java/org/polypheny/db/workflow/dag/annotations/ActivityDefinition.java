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

package org.polypheny.db.workflow.dag.annotations;

import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;
import org.polypheny.db.workflow.dag.activities.Activity.ActivityCategory;
import org.polypheny.db.workflow.dag.activities.Activity.PortType;

@Target(ElementType.TYPE)
@Retention(RetentionPolicy.RUNTIME)
public @interface ActivityDefinition {

    String type();                    // unique identifier for the activity type

    String displayName();            // Display name used by the UI

    /**
     * A short description of this activity shown as raw text.
     */
    String shortDescription() default "";

    /**
     * A long description of this activity using markdown.
     * Image paths are resolved by the UI to "assets/img/plugin/workflows" (subject to change).
     * If no longDescription is given, the shortDescription is used instead.
     */
    String longDescription() default ""; // given as markdown

    ActivityCategory[] categories();

    InPort[] inPorts();

    OutPort[] outPorts();

    String iconPath() default "";               // Path to an icon for display

    @interface InPort {

        PortType type();

        /**
         * A description given in raw text.
         */
        String description() default "";

        boolean isOptional() default false; // TODO: isOptional currently has no effect

    }


    @interface OutPort {

        PortType type();

        /**
         * A description given in raw text.
         */
        String description() default "";

    }

}
