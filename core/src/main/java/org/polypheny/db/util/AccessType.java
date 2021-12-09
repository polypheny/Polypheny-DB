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

package org.polypheny.db.util;


import java.util.EnumSet;
import java.util.Locale;
import org.polypheny.db.algebra.constant.AccessEnum;


/**
 * AccessType is represented by a set of allowed access types
 */
public class AccessType {

    public static final AccessType ALL = new AccessType( EnumSet.allOf( AccessEnum.class ) );
    public static final AccessType READ_ONLY = new AccessType( EnumSet.of( AccessEnum.SELECT ) );
    public static final AccessType WRITE_ONLY = new AccessType( EnumSet.of( AccessEnum.INSERT ) );

    private final EnumSet<AccessEnum> accessEnums;


    public AccessType( EnumSet<AccessEnum> accessEnums ) {
        this.accessEnums = accessEnums;
    }


    public boolean allowsAccess( AccessEnum access ) {
        return accessEnums.contains( access );
    }


    public String toString() {
        return accessEnums.toString();
    }


    public static AccessType create( String[] accessNames ) {
        assert accessNames != null;
        EnumSet<AccessEnum> enumSet = EnumSet.noneOf( AccessEnum.class );
        for ( String accessName : accessNames ) {
            enumSet.add( AccessEnum.valueOf( accessName.trim().toUpperCase( Locale.ROOT ) ) );
        }
        return new AccessType( enumSet );
    }


    public static AccessType create( String accessString ) {
        assert accessString != null;
        accessString = accessString.replace( '[', ' ' );
        accessString = accessString.replace( ']', ' ' );
        String[] accessNames = accessString.split( "," );
        return create( accessNames );
    }

}
