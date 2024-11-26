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
@Repeatable(IntSetting.List.class)
public @interface IntSetting {

    // Common properties
    String key();

    String displayName();

    String description() default "";

    String group() default "";

    String subGroup() default "";

    int position() default 100;

    String subOf() default "";

    // Setting-specific properties

    int defaultValue() default 0;

    boolean isList() default false;  // results in values of type ListValue<IntValue> if true.

    int min() default Integer.MIN_VALUE;

    int max() default Integer.MAX_VALUE;


    @Target(ElementType.TYPE)
    @Retention(RetentionPolicy.RUNTIME)
    @interface List {

        IntSetting[] value();

    }

}