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
 *
 * This file incorporates code covered by the following terms:
 *
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to you under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.polypheny.db.rex;


import org.polypheny.db.util.Glossary;


/**
 * Visitor pattern for traversing a tree of {@link RexNode} objects.
 *
 * @param <R> Return type
 * @see Glossary#VISITOR_PATTERN
 * @see RexShuttle
 * @see RexVisitorImpl
 */
public interface RexVisitor<R> {

    R visitIndexRef( RexIndexRef inputRef );

    R visitLocalRef( RexLocalRef localRef );

    R visitLiteral( RexLiteral literal );

    R visitCall( RexCall call );

    R visitOver( RexOver over );

    R visitCorrelVariable( RexCorrelVariable correlVariable );

    R visitDynamicParam( RexDynamicParam dynamicParam );

    R visitRangeRef( RexRangeRef rangeRef );

    R visitFieldAccess( RexFieldAccess fieldAccess );

    R visitSubQuery( RexSubQuery subQuery );

    R visitTableInputRef( RexTableIndexRef fieldRef );

    R visitPatternFieldRef( RexPatternFieldRef fieldRef );

    R visitNameRef( RexNameRef nameRef );


    R visitElementRef( RexElementRef rexElementRef );

}
