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

package org.polypheny.db.processing;

import java.util.HashSet;
import java.util.Set;
import lombok.Getter;
import org.polypheny.db.rex.RexCall;
import org.polypheny.db.rex.RexDynamicParam;
import org.polypheny.db.rex.RexIndexRef;
import org.polypheny.db.rex.RexNode;
import org.polypheny.db.rex.RexPatternFieldRef;
import org.polypheny.db.rex.RexShuttle;


/**
 * Universal routing rex shuttle class to extract used columns in RelNode.
 */
public class LogicalAlgAnalyzeRexShuttle extends RexShuttle {


    @Getter
    protected final Set<Integer> usedIds = new HashSet<>();


    @Override
    public RexNode visitCall( RexCall call ) {
        super.visitCall( call );
        return call;
    }


    @Override
    public RexNode visitIndexRef( RexIndexRef inputRef ) {
        // Add accessed value
        if ( inputRef != null ) {
            this.usedIds.add( inputRef.getIndex() );
        }

        return super.visitIndexRef( inputRef );
    }


    @Override
    public RexNode visitPatternFieldRef( RexPatternFieldRef fieldRef ) {
        if ( fieldRef != null ) {
            this.usedIds.add( fieldRef.getIndex() );
        }
        return super.visitPatternFieldRef( fieldRef );
    }


    @Override
    public RexNode visitDynamicParam( RexDynamicParam dynamicParam ) {
        if ( dynamicParam != null ) {
            this.usedIds.add( (int) dynamicParam.getIndex() );
        }
        return super.visitDynamicParam( dynamicParam );
    }

}
