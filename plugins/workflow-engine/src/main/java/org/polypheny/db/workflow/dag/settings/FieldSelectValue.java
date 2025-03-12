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
        List<String> remaining = new ArrayList<>( candidates );

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
                    selected.add( insertIdx++, r );
                }
            }
        }
        return selected;
    }


    /**
     * Filters by inspecting labels.
     * If one label should be included and another one excluded, includeConflicting is the tiebreaker.
     * If the set is empty, includeConflicting is returned.
     * A single matching label is sufficient, not all have to match.
     */
    public boolean isSelected( Set<String> labels, boolean includeConflicting ) {
        if ( labels.isEmpty() ) {
            return includeConflicting;
        }
        boolean isExcluded = exclude.stream().anyMatch( labels::contains );
        if ( isExcluded && !includeConflicting ) {
            return false;
        }

        return includeUnspecified() ?
                labels.stream().anyMatch( label -> !exclude.contains( label ) ) :
                include.stream().anyMatch( labels::contains );
    }


    public boolean isSelected( String candidate ) {
        if ( exclude.contains( candidate ) ) {
            return false;
        }
        return includeUnspecified() || include.contains( candidate );
    }

}
