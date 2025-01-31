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

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.databind.JsonNode;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.regex.Pattern;
import java.util.regex.PatternSyntaxException;
import lombok.Value;
import lombok.experimental.NonFinal;
import org.polypheny.db.algebra.AlgCollation;
import org.polypheny.db.algebra.AlgCollations;
import org.polypheny.db.algebra.AlgFieldCollation;
import org.polypheny.db.algebra.AlgFieldCollation.Direction;
import org.polypheny.db.algebra.type.AlgDataType;
import org.polypheny.db.algebra.type.DocumentType;
import org.polypheny.db.rex.RexNameRef;
import org.polypheny.db.rex.RexNode;
import org.polypheny.db.util.Pair;
import org.polypheny.db.workflow.dag.activities.ActivityUtils;
import org.polypheny.db.workflow.dag.settings.SettingDef.SettingValue;

@Value
public class CollationValue implements SettingValue {

    List<FieldCollation> fields;

    @JsonCreator
    public CollationValue(List<FieldCollation> fields) {
        this.fields = fields;
    }


    @Override
    public JsonNode toJson() {
        return MAPPER.valueToTree(fields);
    }


    @Value
    public static class FieldCollation {
        String name;
        Direction direction;
        boolean regex;

        @JsonIgnore
        @NonFinal
        Pattern compiledPattern;

        public Pattern getCompiledPattern() throws PatternSyntaxException {
            if (!regex ) {
                return null;
            }
            if (compiledPattern == null) {
                compiledPattern = Pattern.compile( name );
            }
            return compiledPattern;
        }
    }

    /**
     * For relational entities
     */
    public AlgCollation toAlgCollation( AlgDataType type ) {
        List<String> names = type.getFieldNames();
        List<AlgFieldCollation> collations = new ArrayList<>();
        Set<Integer> included = new HashSet<>();
        for (FieldCollation field : fields) {
            if (field.regex ) {
                for (int idx : ActivityUtils.getRegexMatchPositions(field.getCompiledPattern(), names) ) {
                    if (!included.contains(idx)) {
                        included.add(idx);
                        collations.add( new AlgFieldCollation( idx, field.direction ) );
                    }
                }
            } else {
                int idx = names.indexOf(field.name);
                if (idx == -1 || included.contains(idx)) {
                    continue;
                }
                included.add(idx);
                collations.add( new AlgFieldCollation( idx, field.direction ) );
            }
        }
        return AlgCollations.of(collations);
    };


    /**
     * For collections
     */
    public Pair<AlgCollation, List<RexNode>> toAlgCollation(int inputIndex) {
        //List<String> names = new ArrayList<>();
        List<RexNode> nodes = new ArrayList<>();
        List<AlgFieldCollation> collations = new ArrayList<>();
        int pos = 0;
        for (FieldCollation field : fields) {
            //names.add(field.name);
            nodes.add(new RexNameRef( Arrays.asList( field.name.split( "\\." ) ), inputIndex, DocumentType.ofDoc() ) );
            collations.add( new AlgFieldCollation(pos , field.direction ) );
            pos++;
        }
        return Pair.of(AlgCollations.of( collations ), nodes);
    }

}
