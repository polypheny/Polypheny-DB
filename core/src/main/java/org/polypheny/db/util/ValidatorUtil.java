/*
 * Copyright 2019-2022 The Polypheny Project
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

import static org.polypheny.db.util.Static.RESOURCE;

import com.google.common.base.Utf8;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Iterables;
import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeSet;
import org.apache.calcite.avatica.util.ByteString;
import org.polypheny.db.algebra.core.JoinAlgType;
import org.polypheny.db.algebra.type.AlgDataType;
import org.polypheny.db.algebra.type.AlgDataTypeFactory;
import org.polypheny.db.algebra.type.AlgDataTypeField;
import org.polypheny.db.nodes.Identifier;
import org.polypheny.db.prepare.Prepare.CatalogReader;
import org.polypheny.db.schema.PolyphenyDbSchema;
import org.polypheny.db.schema.PolyphenyDbSchema.TableEntry;
import org.polypheny.db.schema.PolyphenyDbSchema.TypeEntry;
import org.polypheny.db.type.PolyTypeUtil;

public class ValidatorUtil {

    public static final Suggester EXPR_SUGGESTER = ( original, attempt, size ) -> Util.first( original, "EXPR$" ) + attempt;
    public static final Suggester F_SUGGESTER = ( original, attempt, size ) -> Util.first( original, "$f" ) + Math.max( size, attempt );
    public static final Suggester ATTEMPT_SUGGESTER = ( original, attempt, size ) -> Util.first( original, "$" ) + attempt;


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
    public static AlgDataType deriveJoinRowType( AlgDataType leftType, AlgDataType rightType, JoinAlgType joinType, AlgDataTypeFactory typeFactory, List<String> fieldNameList, List<AlgDataTypeField> systemFieldList ) {
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
    public static AlgDataType createJoinType( AlgDataTypeFactory typeFactory, AlgDataType leftType, AlgDataType rightType, List<String> fieldNameList, List<AlgDataTypeField> systemFieldList ) {
        assert (fieldNameList == null)
                || (fieldNameList.size()
                == (systemFieldList.size()
                + leftType.getFieldCount()
                + rightType.getFieldCount()));
        List<String> nameList = new ArrayList<>();
        final List<AlgDataType> typeList = new ArrayList<>();

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


    private static void addFields( List<AlgDataTypeField> fieldList, List<AlgDataType> typeList, List<String> nameList, Set<String> uniqueNames ) {
        for ( AlgDataTypeField field : fieldList ) {
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


    /**
     * Validate if value can be decoded by given charset.
     *
     * @param value nls string in byte array
     * @param charset charset
     * @throws RuntimeException If the given value cannot be represented in the given charset
     */
    public static void validateCharset( ByteString value, Charset charset ) {
        if ( charset == StandardCharsets.UTF_8 ) {
            final byte[] bytes = value.getBytes();
            if ( !Utf8.isWellFormed( bytes ) ) {
                //CHECKSTYLE: IGNORE 1
                final String string = new String( bytes, charset );
                throw RESOURCE.charsetEncoding( string, charset.name() ).ex();
            }
        }
    }


    /**
     * Makes a name distinct from other names which have already been used, adds it to the list, and returns it.
     *
     * @param name Suggested name, may not be unique
     * @param usedNames Collection of names already used
     * @param suggester Base for name when input name is null
     * @return Unique name
     */
    public static String uniquify( String name, Set<String> usedNames, Suggester suggester ) {
        if ( name != null ) {
            if ( usedNames.add( name ) ) {
                return name;
            }
        }
        final String originalName = name;
        for ( int j = 0; ; j++ ) {
            name = suggester.apply( originalName, j, usedNames.size() );
            if ( usedNames.add( name ) ) {
                return name;
            }
        }
    }


    /**
     * Makes sure that the names in a list are unique.
     *
     * Does not modify the input list. Returns the input list if the strings are unique, otherwise allocates a new list.
     *
     * @param nameList List of strings
     * @param caseSensitive Whether upper and lower case names are considered distinct
     * @return List of unique strings
     */
    public static List<String> uniquify( List<String> nameList, boolean caseSensitive ) {
        return uniquify( nameList, EXPR_SUGGESTER, caseSensitive );
    }


    /**
     * Makes sure that the names in a list are unique.
     *
     * Does not modify the input list. Returns the input list if the strings are unique, otherwise allocates a new list.
     *
     * @param nameList List of strings
     * @param suggester How to generate new names if duplicate names are found
     * @param caseSensitive Whether upper and lower case names are considered distinct
     * @return List of unique strings
     */
    public static List<String> uniquify( List<String> nameList, Suggester suggester, boolean caseSensitive ) {
        final Set<String> used = caseSensitive
                ? new LinkedHashSet<>()
                : new TreeSet<>( String.CASE_INSENSITIVE_ORDER );
        int changeCount = 0;
        final List<String> newNameList = new ArrayList<>();
        for ( String name : nameList ) {
            String uniqueName = uniquify( name, used, suggester );
            if ( !uniqueName.equals( name ) ) {
                ++changeCount;
            }
            newNameList.add( uniqueName );
        }
        return changeCount == 0
                ? nameList
                : newNameList;
    }


    /**
     * Returns a map from field names to indexes.
     */
    public static Map<String, Integer> mapNameToIndex( List<AlgDataTypeField> fields ) {
        ImmutableMap.Builder<String, Integer> output = ImmutableMap.builder();
        for ( AlgDataTypeField field : fields ) {
            output.put( field.getName(), field.getIndex() );
        }
        return output.build();
    }


    public static void checkCharsetAndCollateConsistentIfCharType( AlgDataType type ) {
        // (every charset must have a default collation)
        if ( PolyTypeUtil.inCharFamily( type ) ) {
            Charset strCharset = type.getCharset();
            Charset colCharset = type.getCollation().getCharset();
            assert null != strCharset;
            assert null != colCharset;
            if ( !strCharset.equals( colCharset ) ) {
                if ( false ) {
                    // todo: enable this checking when we have a charset to collation mapping
                    throw new Error( type.toString() + " was found to have charset '" + strCharset.name() + "' and a mismatched collation charset '" + colCharset.name() + "'" );
                }
            }
        }
    }


    /**
     * Finds a {@link TypeEntry} in a given schema whose type has the given name, possibly qualified.
     *
     * @param rootSchema root schema
     * @param typeName name of the type, may be qualified or fully-qualified
     * @return TypeEntry with a table with the given name, or null
     */
    public static TypeEntry getTypeEntry( PolyphenyDbSchema rootSchema, Identifier typeName ) {
        final String name;
        final List<String> path;
        if ( typeName.isSimple() ) {
            path = ImmutableList.of();
            name = typeName.getSimple();
        } else {
            path = Util.skipLast( typeName.getNames() );
            name = Util.last( typeName.getNames() );
        }
        PolyphenyDbSchema schema = rootSchema;
        for ( String p : path ) {
            if ( schema == rootSchema && NameMatchers.withCaseSensitive( true ).matches( p, schema.getName() ) ) {
                continue;
            }
            schema = schema.getSubSchema( p, true );
        }
        return schema == null ? null : schema.getType( name, false );
    }


    /**
     * Finds a {@link TableEntry} in a given catalog reader whose table has the given name, possibly qualified.
     *
     * Uses the case-sensitivity policy of the specified catalog reader.
     *
     * If not found, returns null.
     *
     * @param catalogReader accessor to the table metadata
     * @param names Name of table, may be qualified or fully-qualified
     * @return TableEntry with a table with the given name, or null
     */
    public static TableEntry getTableEntry( CatalogReader catalogReader, List<String> names ) {
        // First look in the default schema, if any.
        // If not found, look in the root schema.
        for ( List<String> schemaPath : catalogReader.getSchemaPaths() ) {
            PolyphenyDbSchema schema =
                    getSchema(
                            catalogReader.getRootSchema(),
                            Iterables.concat( schemaPath, Util.skipLast( names ) ),
                            catalogReader.nameMatcher() );
            if ( schema == null ) {
                continue;
            }
            TableEntry entry = getTableEntryFrom( schema, Util.last( names ), catalogReader.nameMatcher().isCaseSensitive() );
            if ( entry != null ) {
                return entry;
            }
        }
        return null;
    }


    /**
     * Finds and returns {@link PolyphenyDbSchema} nested to the given rootSchema with specified schemaPath.
     *
     * Uses the case-sensitivity policy of specified nameMatcher.
     *
     * If not found, returns null.
     *
     * @param rootSchema root schema
     * @param schemaPath full schema path of required schema
     * @param nameMatcher name matcher
     * @return PolyphenyDbSchema that corresponds specified schemaPath
     */
    public static PolyphenyDbSchema getSchema( PolyphenyDbSchema rootSchema, Iterable<String> schemaPath, NameMatcher nameMatcher ) {
        PolyphenyDbSchema schema = rootSchema;
        for ( String schemaName : schemaPath ) {
            if ( schema == rootSchema && nameMatcher.matches( schemaName, schema.getName() ) ) {
                continue;
            }
            schema = schema.getSubSchema( schemaName, nameMatcher.isCaseSensitive() );
            if ( schema == null ) {
                return null;
            }
        }
        return schema;
    }


    private static TableEntry getTableEntryFrom( PolyphenyDbSchema schema, String name, boolean caseSensitive ) {
        TableEntry entry = schema.getTable( name );
        if ( entry == null ) {
            entry = schema.getTableBasedOnNullaryFunction( name, caseSensitive );
        }
        return entry;
    }


    public static AlgDataType createTypeFromProjection( AlgDataType type, List<String> columnNameList, AlgDataTypeFactory typeFactory, boolean caseSensitive ) {
        // If the names in columnNameList and type have case-sensitive differences, the resulting type will use those from type. These are presumably more canonical.
        final List<AlgDataTypeField> fields = new ArrayList<>( columnNameList.size() );
        for ( String name : columnNameList ) {
            AlgDataTypeField field = type.getField( name, caseSensitive, false );
            fields.add( type.getFieldList().get( field.getIndex() ) );
        }
        return typeFactory.createStructType( fields );
    }


    /**
     * Suggests candidates for unique names, given the number of attempts so far and the number of expressions in the project list.
     */
    public interface Suggester {

        String apply( String original, int attempt, int size );

    }

}
