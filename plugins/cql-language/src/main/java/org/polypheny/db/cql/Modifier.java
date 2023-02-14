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

package org.polypheny.db.cql;


/**
 * Packaging information used to modify an operation.
 * Used in query relations, filters, sort specifications
 * and projections.
 */
public class Modifier {

    public final String modifierName;
    public final Comparator comparator;
    public final String modifierValue;


    public Modifier( final String modifierName, final Comparator comparator, final String modifierValue ) {
        this.modifierName = modifierName;
        this.comparator = comparator;
        this.modifierValue = modifierValue;
    }


    public Modifier( final String modifierName ) {
        this.modifierName = modifierName;
        this.comparator = Comparator.SERVER_CHOICE;
        this.modifierValue = "";
    }


    @Override
    public String toString() {
        return "/ " + modifierName + " " + comparator + " " + modifierValue;
    }

}
