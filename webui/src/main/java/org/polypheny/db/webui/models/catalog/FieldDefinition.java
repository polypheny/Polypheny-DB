/*
 * Copyright 2019-2023 The Polypheny Project
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

package org.polypheny.db.webui.models.catalog;

import lombok.Value;
import lombok.experimental.Accessors;
import lombok.experimental.NonFinal;
import lombok.experimental.SuperBuilder;
import org.polypheny.db.algebra.type.AlgDataTypeField;

@Value
@Accessors(chain = true)
@SuperBuilder
@NonFinal
public class FieldDefinition {

    public String name;
    // for both
    public String dataType; //varchar/int/etc


    public static FieldDefinition of( AlgDataTypeField field ) {
        return FieldDefinition.builder().name( field.getName() ).dataType( field.getType().getFullTypeString() ).build();
    }

}
