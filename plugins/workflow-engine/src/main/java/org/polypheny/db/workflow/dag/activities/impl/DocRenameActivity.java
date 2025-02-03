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

package org.polypheny.db.workflow.dag.activities.impl;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import org.polypheny.db.algebra.type.AlgDataType;
import org.polypheny.db.type.PolyType;
import org.polypheny.db.type.entity.PolyList;
import org.polypheny.db.type.entity.PolyString;
import org.polypheny.db.type.entity.PolyValue;
import org.polypheny.db.type.entity.document.PolyDocument;
import org.polypheny.db.workflow.dag.activities.Activity;
import org.polypheny.db.workflow.dag.activities.Activity.ActivityCategory;
import org.polypheny.db.workflow.dag.activities.Activity.PortType;
import org.polypheny.db.workflow.dag.activities.ActivityException;
import org.polypheny.db.workflow.dag.activities.ActivityException.InvalidSettingException;
import org.polypheny.db.workflow.dag.activities.ActivityUtils;
import org.polypheny.db.workflow.dag.activities.Pipeable;
import org.polypheny.db.workflow.dag.activities.TypePreview;
import org.polypheny.db.workflow.dag.activities.TypePreview.DocType;
import org.polypheny.db.workflow.dag.annotations.ActivityDefinition;
import org.polypheny.db.workflow.dag.annotations.ActivityDefinition.InPort;
import org.polypheny.db.workflow.dag.annotations.ActivityDefinition.OutPort;
import org.polypheny.db.workflow.dag.annotations.BoolSetting;
import org.polypheny.db.workflow.dag.annotations.FieldRenameSetting;
import org.polypheny.db.workflow.dag.annotations.StringSetting;
import org.polypheny.db.workflow.dag.settings.FieldRenameValue;
import org.polypheny.db.workflow.dag.settings.FieldRenameValue.RenameMode;
import org.polypheny.db.workflow.dag.settings.SettingDef.Settings;
import org.polypheny.db.workflow.dag.settings.SettingDef.SettingsPreview;
import org.polypheny.db.workflow.dag.settings.StringSettingDef.AutoCompleteType;
import org.polypheny.db.workflow.engine.execution.context.PipeExecutionContext;
import org.polypheny.db.workflow.engine.execution.pipe.InputPipe;
import org.polypheny.db.workflow.engine.execution.pipe.OutputPipe;

@ActivityDefinition(type = "docRename", displayName = "Rename Document Fields", categories = { ActivityCategory.TRANSFORM, ActivityCategory.DOCUMENT },
        inPorts = { @InPort(type = PortType.DOC, description = "The input collection.") },
        outPorts = { @OutPort(type = PortType.DOC, description = "The input collection with renamed (sub)fields.") },
        shortDescription = "Rename the (sub)fields of a collection by defining rules."
)
@StringSetting(key = "pointer", displayName = "Relative Path", pos = 0, autoCompleteType = AutoCompleteType.FIELD_NAMES,
        shortDescription = "Optionally specify a (sub)field such as 'owner.address'. All rules will act relative to this field.")
@FieldRenameSetting(key = "rename", displayName = "Renaming Rules", allowRegex = true, allowIndex = false, pos = 1,
        shortDescription = "The source fields can be selected by their actual (constant) name or with Regex. "
                + "The replacement can reference capture groups such as '$0' for the original name.",
        longDescription = """
                The source fields can be selected by their actual (constant) name or by using a regular expression.
                In constant mode, subfields can be specified using dots to separate fields in the search string.
                Regex mode can be used to specify capturing groups using parentheses.
                
                In any mode, the replacement can reference a capture group (`$0`, `$1`...). For instance, the replacement `abc$0` adds the prefix `abc` to a field name.
                
                Regular expressions are given in the [Java Regex dialect](https://docs.oracle.com/javase/8/docs/api/java/util/regex/Pattern.html).
                """)
@BoolSetting(key = "all", displayName = "Match Subfields", pos = 2, subPointer = "rename/mode", subValues = { "\"REGEX\"" }, defaultValue = false,
        shortDescription = "If enabled, the search extends to all subfields.",
        longDescription = """
                If enabled, the search extends to all subfields.
                
                To instead target specific subfields, use the `Relative Path` setting or change the mode to `Constant`.
                """)

@SuppressWarnings("unused")
public class DocRenameActivity implements Activity, Pipeable {

    private final Map<String, String> renameCache = new HashMap<>();
    private FieldRenameValue renamer;


    @Override
    public List<TypePreview> previewOutTypes( List<TypePreview> inTypes, SettingsPreview settings ) throws ActivityException {

        if ( inTypes.get( 0 ) instanceof DocType docType && settings.keysPresent( "pointer", "rename" ) ) {
            String pointer = settings.getString( "pointer" );
            Set<String> fields = docType.getKnownFields();
            if ( !pointer.isBlank() ) {
                return DocType.of( fields ).asOutTypes();
            }
            FieldRenameValue rename = settings.getOrThrow( "rename", FieldRenameValue.class );
            return DocType.of( rename.getRenamedSet( fields ) ).asOutTypes();
        }
        return DocType.of().asOutTypes();
    }


    @Override
    public AlgDataType lockOutputType( List<AlgDataType> inTypes, Settings settings ) throws Exception {
        return getDocType();
    }


    @Override
    public void pipe( List<InputPipe> inputs, OutputPipe output, Settings settings, PipeExecutionContext ctx ) throws Exception {
        boolean hasPointer = !settings.getString( "pointer" ).isEmpty();
        List<String> pointer = hasPointer ? Arrays.asList( settings.getString( "pointer" ).split( "\\." ) ) : null;
        renamer = settings.get( "rename", FieldRenameValue.class );

        if ( renamer.getMode() == RenameMode.REGEX ) {
            boolean all = settings.getBool( "all" );
            for ( List<PolyValue> row : inputs.get( 0 ) ) {
                PolyDocument doc = row.get( 0 ).asDocument();
                PolyValue renamedDoc;
                try {
                    PolyValue root = hasPointer ? getPointerValue( doc, pointer ) : doc;
                    PolyValue renamedRoot = renameRecursive( root, all );
                    renamedDoc = hasPointer ? replaceRecursive( doc, pointer, renamedRoot ) : renamedRoot;
                } catch ( IllegalArgumentException e ) {
                    throw new InvalidSettingException( e.getMessage(), "rename" ); // invalid field name
                } catch ( Exception e ) {
                    renamedDoc = doc; // invalid paths should not result in a failure. Instead, we do not rename anything
                }
                if ( !output.put( renamedDoc ) ) {
                    finish( inputs );
                    return;
                }
            }

        } else {
            // constant mode
            for ( List<PolyValue> row : inputs.get( 0 ) ) {
                PolyDocument doc = row.get( 0 ).asDocument();
                PolyValue renamedDoc;
                try {
                    PolyValue root = hasPointer ? getPointerValue( doc, pointer ) : doc;
                    PolyValue renamedRoot = renameRecursive( "", root );
                    renamedDoc = hasPointer ? replaceRecursive( doc, pointer, renamedRoot ) : renamedRoot;
                } catch ( IllegalArgumentException e ) {
                    throw new InvalidSettingException( e.getMessage(), "rename" ); // invalid field name
                } catch ( Exception e ) {
                    renamedDoc = doc; // invalid paths should not result in a failure. Instead, we do not rename anything
                }
                if ( !output.put( renamedDoc ) ) {
                    finish( inputs );
                    return;
                }
            }
        }
    }


    /**
     * Resolve the subfield pointer.
     *
     * @param doc root document
     * @param pointer the pointer divided into its parts. Must not be empty.
     * @return a PolyDocument or a PolyList
     * @throws Exception if the target either does not exist or has an invalid PolyType
     */
    private PolyValue getPointerValue( PolyDocument doc, List<String> pointer ) throws Exception {
        PolyValue current = doc;
        for ( String s : pointer ) {
            current = switch ( current.getType() ) {
                case DOCUMENT -> {
                    PolyString next = PolyString.of( s );
                    yield current.asDocument().get( next );
                }
                case ARRAY -> {
                    int next = Integer.parseInt( s );
                    yield current.asList().get( next );
                }
                default -> throw new IllegalStateException( "Unexpected type: " + current.getType() );
            };
        }
        if ( current.getType() != PolyType.DOCUMENT && current.getType() != PolyType.ARRAY ) {
            throw new IllegalStateException( "Unexpected type: " + current.getType() );
        }
        return current;
    }


    /**
     * For constant renaming (search string can be path)
     */
    private PolyValue renameRecursive( String parentPath, PolyValue value ) throws IllegalArgumentException {
        return switch ( value.getType() ) {
            case DOCUMENT -> {
                PolyDocument doc = value.asDocument();
                Map<PolyString, PolyValue> map = new HashMap<>();
                for ( Entry<PolyString, PolyValue> entry : doc.entrySet() ) {
                    String key = entry.getKey().value;
                    String path = (parentPath.isEmpty() ? "" : (parentPath + ".")) + entry.getKey().value;
                    PolyValue renamedValue = renameRecursive( path, entry.getValue() );

                    String renamed = getRenamed( path, key );
                    map.put( PolyString.of( renamed ), renamedValue );
                }
                yield PolyDocument.ofDocument( map );
            }
            case ARRAY -> {
                PolyList<?> list = value.asList();
                List<PolyValue> renamed = new ArrayList<>();
                for ( int i = 0; i < list.size(); i++ ) {
                    String path = (parentPath.isEmpty() ? "" : (parentPath + ".")) + i;
                    renamed.add( renameRecursive( path, list.get( i ) ) );
                }
                yield PolyList.of( renamed );
            }
            default -> value;
        };
    }


    /**
     * For regex renaming.
     * if nested: searches all subfields for matches (corresponds to "all" setting)
     */
    private PolyValue renameRecursive( PolyValue value, boolean nested ) throws IllegalArgumentException {
        return switch ( value.getType() ) {
            case DOCUMENT -> {
                PolyDocument doc = value.asDocument();
                Map<PolyString, PolyValue> map = new HashMap<>();
                for ( Entry<PolyString, PolyValue> entry : doc.entrySet() ) {
                    String key = entry.getKey().value;
                    PolyValue renamedValue = nested ? renameRecursive( entry.getValue(), true ) : entry.getValue();

                    String renamed = getRenamed( key, key );
                    map.put( PolyString.of( renamed ), renamedValue );
                }
                yield PolyDocument.ofDocument( map );
            }
            case ARRAY -> {
                PolyList<?> list = value.asList();
                List<PolyValue> renamed = new ArrayList<>();
                for ( PolyValue polyValue : list ) {
                    renamed.add( renameRecursive( polyValue, nested ) );
                }
                yield PolyList.of( renamed );
            }
            default -> value;
        };
    }


    private String getRenamed( String name, String defaultValue ) throws IllegalArgumentException {
        return renameCache.computeIfAbsent( name, k -> {
            String r = renamer.rename( k );
            if ( r != null ) {
                if ( !ActivityUtils.isValidFieldName( r ) ) {
                    throw new IllegalArgumentException( "Invalid field name: " + r );
                }
                return r;
            }
            return defaultValue;
        } );
    }


    private PolyValue replaceRecursive( PolyValue root, List<String> pointer, PolyValue replacement ) {
        if ( pointer.isEmpty() ) {
            return replacement;
        }
        List<String> remaining = pointer.subList( 1, pointer.size() );
        return switch ( root.getType() ) {
            case DOCUMENT -> {
                PolyString next = PolyString.of( pointer.get( 0 ) );
                PolyDocument doc = root.asDocument();
                Map<PolyString, PolyValue> map = new HashMap<>( doc.map );
                map.computeIfPresent( next, ( k, v ) -> replaceRecursive( v, remaining, replacement ) );
                yield PolyDocument.ofDocument( map );
            }
            case ARRAY -> {
                int i = Integer.parseInt( pointer.get( 0 ) );
                PolyList<?> list = root.asList();
                List<PolyValue> replaced = new ArrayList<>( list );
                replaced.set( i, replaceRecursive( replaced.get( i ), remaining, replacement ) );
                yield PolyList.of( replaced );
            }
            default -> throw new IllegalStateException( "Unexpected value: " + root );
        };
    }


    @Override
    public void reset() {
        renameCache.clear();
        renamer = null;
    }

}
