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

@Target(ElementType.TYPE)
@Retention(RetentionPolicy.RUNTIME)
@Repeatable(StringSetting.List.class)
public @interface StringSetting {

    /**
     * A unique key that identifies this setting.
     * Must not contain {@code SettingDef.SUB_SEP}.
     *
     * @return the key of this setting
     */
    String key();

    String displayName();

    String description() default "";

    String group() default "";  // the group this setting belongs to. Default is {@code GroupDef.DEFAULT_GROUP}, advanced is {@code GroupDef.ADVANCED_GROUP}. Others must be created manually

    String subGroup() default "";  // the subgroup this setting belongs to. Default is {@code GroupDef.DEFAULT_SUBGROUP}

    int position() default 100;  // manually impose order within subGroup (lower pos => further to the top)

    String subOf() default "";  // determine the visibility of this setting, possibly using {@code SettingDef.SUB_SEP}.

    // String-specific settings
    String defaultValue() default "";

    boolean isList() default false;  // results in values of type ListValue<StringValue> if true.

    /**
     * The minimum length of the string (inclusive)
     */
    int minLength() default 0;

    /**
     * The maximum length of the string (exclusive)
     */
    int maxLength() default Integer.MAX_VALUE;


    @Target(ElementType.TYPE)
    @Retention(RetentionPolicy.RUNTIME)
    @interface List {

        StringSetting[] value();

    }

}
