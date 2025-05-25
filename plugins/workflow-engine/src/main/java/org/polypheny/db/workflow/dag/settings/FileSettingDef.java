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

package org.polypheny.db.workflow.dag.settings;

import com.fasterxml.jackson.databind.JsonNode;
import java.util.Arrays;
import java.util.List;
import lombok.EqualsAndHashCode;
import lombok.Value;
import org.polypheny.db.workflow.dag.activities.ActivityException.InvalidSettingException;
import org.polypheny.db.workflow.dag.annotations.FileSetting;
import org.polypheny.db.workflow.dag.settings.FileValue.SourceType;

@EqualsAndHashCode(callSuper = true)
@Value
public class FileSettingDef extends SettingDef {

    boolean allowMultiple;
    List<SourceType> modes;


    public FileSettingDef( FileSetting a ) {
        super( SettingType.FILE, a.key(), a.displayName(), a.shortDescription(), a.longDescription(),
                getDefaultValue( a.defaultPath(), a.modes() ),
                a.group(), a.subGroup(), a.pos(), a.subPointer(), a.subValues() );
        allowMultiple = a.multi();
        modes = Arrays.asList( a.modes() );
    }


    @Override
    public SettingValue buildValue( JsonNode node ) {
        return SettingValue.fromJson( node, FileValue.class );
    }


    @Override
    public void validateValue( SettingValue value ) throws InvalidSettingException {
        if ( value instanceof FileValue fileValue ) {
            if ( !modes.contains( fileValue.getType() ) ) {
                throwInvalid( "Specified source type is not permitted: " + fileValue.getType() );
            }

            try {
                fileValue.validate( allowMultiple );
            } catch ( IllegalArgumentException e ) {
                throwInvalid( e.getMessage() );
            }
            return;
        }
        throw new IllegalArgumentException( "Value is not a FileValue" );
    }


    private static SettingValue getDefaultValue( String path, SourceType[] modes ) {
        return new FileValue( path, modes[0], false );
    }

}
