/*
 * Copyright 2019-2025 The Polypheny Project
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
import org.polypheny.db.workflow.dag.settings.StringSettingDef.AutoCompleteType;

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

    /**
     * A short description given in raw text.
     */
    String shortDescription() default "";

    /**
     * A description given in markdown.
     */
    String longDescription() default "";

    String group() default "";  // the group this setting belongs to. Default is {@code GroupDef.DEFAULT_GROUP}, advanced is {@code GroupDef.ADVANCED_GROUP}. Others must be created manually

    String subGroup() default "";  // the subgroup this setting belongs to. Default is {@code GroupDef.DEFAULT_SUBGROUP}

    int pos() default SettingDef.DEFAULT_POS;  // manually impose order within subGroup (lower pos => further to the top)

    /**
     * See {@link SettingDef#getSubPointer()}
     */
    String subPointer() default "";

    /**
     * See {@link SettingDef#getSubValues()}
     */
    String[] subValues() default {};

    // String-specific settings
    String defaultValue() default "";


    /**
     * The minimum length of the string (inclusive)
     */
    int minLength() default 0;

    /**
     * The maximum length of the string (exclusive)
     */
    int maxLength() default Integer.MAX_VALUE;

    /**
     * Whether the string must consist of at least one non-whitespace character
     */
    boolean nonBlank() default false;


    AutoCompleteType autoCompleteType() default AutoCompleteType.NONE;

    /**
     * The input to use for autoComplete hints.
     * Only has an effect if autoCompleteType != NONE.
     */
    int autoCompleteInput() default 0;

    boolean containsRegex() default false;

    /**
     * Whether to display a text editor in the UI.
     * It is better suited for longer (multi-line) strings.
     */
    boolean textEditor() default false;

    /**
     * The syntax highlighting language when using the text editor.
     */
    String language() default "";

    /**
     * Whether to show line numbers when using the text editor.
     */
    boolean lineNumbers() default false;


    @Target(ElementType.TYPE)
    @Retention(RetentionPolicy.RUNTIME)
    @interface List {

        StringSetting[] value();

    }

}
