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

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import lombok.Value;
import org.polypheny.db.workflow.dag.settings.SettingDef.SettingValue;

@Value
public class FieldSelectValue implements SettingValue {

    List<String> include;
    List<String> exclude;
    /**
     * If <0 unspecified fields are excluded by default.
     * If >0 unspecified fields are inserted at the given index
     */
    int unspecifiedIndex;


    public boolean includeUnspecified() {
        return unspecifiedIndex >= 0;
    }


    public List<String> getSelected( Collection<String> candidates ) {
        List<String> selected = new ArrayList<>();
        Set<String> remaining = new HashSet<>( candidates );

        /*if (includeUnspecified()) {
            for (int i = 0; i < Math.min(unspecifiedIndex, include.size()); i++) {
                String in  = include.get(i);
                if (remaining.contains(in)) {
                    if (!exclude.contains(in)) {
                        selected.add(in);
                    }
                    remaining.remove(in);
                }
            }
            for (String candidate : candidates) {
                if (remaining.contains(candidate) && !include.contains(candidate)) {
                    if (!exclude.contains(candidate)) {
                        selected.add(candidate);
                    }
                }
            }
        }*/

        int insertIdx = -1;
        for ( int i = 0; i < include.size(); i++ ) {
            if ( i == unspecifiedIndex ) {
                insertIdx = selected.size();
            }
            String in = include.get( i );
            if ( remaining.contains( in ) ) {
                selected.add( in );
                remaining.remove( in );
            }
        }
        if ( includeUnspecified() ) {
            if ( insertIdx == -1 ) {
                insertIdx = selected.size();
            }
            for ( String r : remaining ) {
                if ( !exclude.contains( r ) ) {
                    selected.add( insertIdx, r );
                }
            }
        }
        return selected;
    }

}
