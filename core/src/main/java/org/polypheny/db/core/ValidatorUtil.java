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

package org.polypheny.db.core;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.TreeSet;
import org.polypheny.db.rel.core.JoinRelType;
import org.polypheny.db.rel.type.RelDataType;
import org.polypheny.db.rel.type.RelDataTypeFactory;
import org.polypheny.db.rel.type.RelDataTypeField;

public class ValidatorUtil {

    /**
     * Derives the type of a join relational expression.
     *
     * @param leftType Row type of left input to join
     * @param rightType Row type of right input to join
     * @param joinType Type of join
     * @param typeFactory Type factory
     * @param fieldNameList List of names of fields; if null, field names are inherited and made unique
     * @param systemFieldList List of system fields that will be prefixed to output row type; typically empty but must not be null
     * @return join type
     */
    public static RelDataType deriveJoinRowType( RelDataType leftType, RelDataType rightType, JoinRelType joinType, RelDataTypeFactory typeFactory, List<String> fieldNameList, List<RelDataTypeField> systemFieldList ) {
        assert systemFieldList != null;
        switch ( joinType ) {
            case LEFT:
                rightType = typeFactory.createTypeWithNullability( rightType, true );
                break;
            case RIGHT:
                leftType = typeFactory.createTypeWithNullability( leftType, true );
                break;
            case FULL:
                leftType = typeFactory.createTypeWithNullability( leftType, true );
                rightType = typeFactory.createTypeWithNullability( rightType, true );
                break;
            default:
                break;
        }
        return createJoinType( typeFactory, leftType, rightType, fieldNameList, systemFieldList );
    }


    /**
     * Returns the type the row which results when two relations are joined.
     *
     * The resulting row type consists of the system fields (if any), followed by the fields of the left type, followed by the fields of the right type. The field name list, if present, overrides the original names of the fields.
     *
     * @param typeFactory Type factory
     * @param leftType Type of left input to join
     * @param rightType Type of right input to join, or null for semi-join
     * @param fieldNameList If not null, overrides the original names of the fields
     * @param systemFieldList List of system fields that will be prefixed to output row type; typically empty but must not be null
     * @return type of row which results when two relations are joined
     */
    public static RelDataType createJoinType( RelDataTypeFactory typeFactory, RelDataType leftType, RelDataType rightType, List<String> fieldNameList, List<RelDataTypeField> systemFieldList ) {
        assert (fieldNameList == null)
                || (fieldNameList.size()
                == (systemFieldList.size()
                + leftType.getFieldCount()
                + rightType.getFieldCount()));
        List<String> nameList = new ArrayList<>();
        final List<RelDataType> typeList = new ArrayList<>();

        // Use a set to keep track of the field names; this is needed to ensure that the contains() call to check for name uniqueness runs in constant time; otherwise, if the number of fields is large, doing a contains() on a list can be expensive.
        final Set<String> uniqueNameList =
                typeFactory.getTypeSystem().isSchemaCaseSensitive()
                        ? new HashSet<>()
                        : new TreeSet<>( String.CASE_INSENSITIVE_ORDER );
        addFields( systemFieldList, typeList, nameList, uniqueNameList );
        addFields( leftType.getFieldList(), typeList, nameList, uniqueNameList );
        if ( rightType != null ) {
            addFields( rightType.getFieldList(), typeList, nameList, uniqueNameList );
        }
        if ( fieldNameList != null ) {
            assert fieldNameList.size() == nameList.size();
            nameList = fieldNameList;
        }
        return typeFactory.createStructType( typeList, nameList );
    }


    private static void addFields( List<RelDataTypeField> fieldList, List<RelDataType> typeList, List<String> nameList, Set<String> uniqueNames ) {
        for ( RelDataTypeField field : fieldList ) {
            String name = field.getName();

            // Ensure that name is unique from all previous field names
            if ( uniqueNames.contains( name ) ) {
                String nameBase = name;
                for ( int j = 0; ; j++ ) {
                    name = nameBase + j;
                    if ( !uniqueNames.contains( name ) ) {
                        break;
                    }
                }
            }
            nameList.add( name );
            uniqueNames.add( name );
            typeList.add( field.getType() );
        }
    }

}
