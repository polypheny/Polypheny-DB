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

package org.polypheny.db.router;

import java.util.HashSet;
import lombok.Getter;
import org.polypheny.db.rex.RexCall;
import org.polypheny.db.rex.RexDynamicParam;
import org.polypheny.db.rex.RexInputRef;
import org.polypheny.db.rex.RexNode;
import org.polypheny.db.rex.RexPatternFieldRef;
import org.polypheny.db.rex.RexShuttle;
import org.polypheny.db.transaction.Statement;

/**
 * Unified routing rex shuttle class to extract used columns in RelNode
 */
public class QueryAnalyzeRexShuttle extends RexShuttle {


    @Getter
    protected final HashSet<Integer> ids = new HashSet<>();
    private final Statement statement;


    public QueryAnalyzeRexShuttle( Statement statement ) {
        super();
        this.statement = statement;
    }


    @Override
    public RexNode visitCall( RexCall call ) {
        super.visitCall( call );
        return call;
    }


    @Override
    public RexNode visitInputRef( RexInputRef inputRef ) {
        // add accessed value
        if ( inputRef != null ) {
            this.ids.add( inputRef.getIndex() );
        }

        return super.visitInputRef( inputRef );
    }


    @Override
    public RexNode visitPatternFieldRef( RexPatternFieldRef fieldRef ) {
        if ( fieldRef != null ) {
            this.ids.add( fieldRef.getIndex() );
        }
        return super.visitPatternFieldRef( fieldRef );
    }


    @Override
    public RexNode visitDynamicParam( RexDynamicParam dynamicParam ) {
        if ( dynamicParam != null ) {
            this.ids.add( (int) dynamicParam.getIndex() );
        }
        return super.visitDynamicParam( dynamicParam );
    }


}
