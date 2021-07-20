/*
 * Copyright 2019-2021 The Polypheny Project
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

package org.polypheny.cql.contextset;

public enum ModifierType {

    BOOLEAN( false, true ),
    STRING( false, true ),
    NUMBER( true, true ),
    ENUM( false, true );

    public boolean COMPARABLE;
    public boolean ASSIGNABLE;


    ModifierType( boolean comparable, boolean assignable ) {
        this.COMPARABLE = comparable;
        this.ASSIGNABLE = assignable;
    }

}
