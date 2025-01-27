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
import java.lang.annotation.Repeatable;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;
import org.polypheny.db.workflow.dag.settings.SettingDef;

@Target(ElementType.TYPE)
@Retention(RetentionPolicy.RUNTIME)
@Repeatable(EnumSetting.List.class)
public @interface EnumSetting {

    /**
     * A unique key that identifies this setting.
     * Must not contain {@code SettingDef.SUB_SEP}.
     *
     * @return the key of this setting
     */
    String key();

    String displayName();

    /**
     * A short description given in raw text.
     */
    String shortDescription() default "";

    /**
     * A description given in markdown.
     */
    String longDescription() default "";

    String group() default "";

    String subGroup() default "";

    int position() default 100;

    /**
     * See {@link SettingDef#getSubPointer()}
     */
    String subPointer() default "";

    /**
     * See {@link SettingDef#getSubValues()}
     */
    String[] subValues() default {};

    // Setting-specific properties
    String[] options();
    String[] displayOptions() default {};
    String defaultValue();
    String label() default "";


    @Target(ElementType.TYPE)
    @Retention(RetentionPolicy.RUNTIME)
    @interface List {

        EnumSetting[] value();

    }

}
