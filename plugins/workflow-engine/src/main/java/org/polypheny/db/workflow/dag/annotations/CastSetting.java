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
import org.polypheny.db.type.PolyType;
import org.polypheny.db.workflow.dag.settings.SettingDef;

@Target(ElementType.TYPE)
@Retention(RetentionPolicy.RUNTIME)
@Repeatable(CastSetting.List.class)
public @interface CastSetting {

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

    int pos() default SettingDef.DEFAULT_POS;

    /**
     * See {@link SettingDef#getSubPointer()}
     */
    String subPointer() default "";

    /**
     * See {@link SettingDef#getSubValues()}
     */
    String[] subValues() default {};

    // Setting-specifics
    PolyType defaultType();

    int targetInput() default 0;

    boolean duplicateSource() default false;

    /**
     * Allows user to specify the name of the target field
     */
    boolean allowTarget() default false;

    /**
     * Allows users to mark a cast as "JSON".
     * Any type information is ignored for such casts (TEXT is always used).
     */
    boolean allowJson() default false;

    /**
     * If true, the value is valid iff there is a single cast.
     */
    boolean singleCast() default false;


    @Target(ElementType.TYPE)
    @Retention(RetentionPolicy.RUNTIME)
    @interface List {

        CastSetting[] value();

    }

}
