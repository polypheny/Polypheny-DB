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

package org.polypheny.db.tools;


import java.io.Reader;
import org.polypheny.db.algebra.AlgNode;
import org.polypheny.db.algebra.AlgRoot;
import org.polypheny.db.algebra.type.AlgDataType;
import org.polypheny.db.algebra.type.AlgDataTypeFactory;
import org.polypheny.db.languages.NodeParseException;
import org.polypheny.db.nodes.Node;
import org.polypheny.db.plan.AlgTraitSet;
import org.polypheny.db.util.Pair;
import org.polypheny.db.util.SourceStringReader;


/**
 * A fa&ccedil;ade that covers Polypheny-DB's query planning process: parse SQL, validate the parse tree, convert the parse
 * tree to a relational expression, and optimize the relational expression.
 * <p>
 * Planner is NOT thread safe. However, it can be reused for different queries. The consumer of this interface is responsible
 * for calling reset() after each use of Planner that corresponds to a different query.
 */
public interface Planner extends AutoCloseable {

    /**
     * Parses and validates a SQL statement.
     *
     * @param sql The SQL statement to parse.
     * @return The root node of the SQL parse tree.
     * @throws NodeParseException on parse error
     */
    default Node parse( String sql ) throws NodeParseException {
        return parse( new SourceStringReader( sql ) );
    }

    /**
     * Parses and validates a SQL statement.
     *
     * @param source A reader which will provide the SQL statement to parse.
     * @return The root node of the SQL parse tree.
     * @throws NodeParseException on parse error
     */
    Node parse( Reader source ) throws NodeParseException;

    /**
     * Validates a SQL statement.
     *
     * @param sqlNode Root node of the SQL parse tree.
     * @return Validated node
     * @throws ValidationException if not valid
     */
    Node validate( Node sqlNode ) throws ValidationException;

    /**
     * Validates a SQL statement.
     *
     * @param sqlNode Root node of the SQL parse tree.
     * @return Validated node and its validated type.
     * @throws ValidationException if not valid
     */
    Pair<Node, AlgDataType> validateAndGetType( Node sqlNode ) throws ValidationException;

    /**
     * Converts a SQL parse tree into a tree of relational expressions.
     *
     * You must call {@link #validate(Node)} first.
     *
     * @param sql The root node of the SQL parse tree.
     * @return The root node of the newly generated {@link AlgNode} tree.
     * @throws AlgConversionException if the node cannot be converted or has not been validated
     */
    AlgRoot alg( Node sql ) throws AlgConversionException;

    /**
     * Returns the type factory.
     */
    AlgDataTypeFactory getTypeFactory();

    /**
     * Converts one relational expression tree into another relational expression based on a particular rule set and requires set of traits.
     *
     * @param ruleSetIndex The RuleSet to use for conversion purposes.  Note that this is zero-indexed and is based on the list and order of RuleSets provided in the construction of this Planner.
     * @param requiredOutputTraits The set of RelTraits required of the root node at the termination of the planning cycle.
     * @param alg The root of the {@link AlgNode} tree to convert.
     * @return The root of the new {@link AlgNode} tree.
     * @throws AlgConversionException on conversion error
     */
    AlgNode transform( int ruleSetIndex, AlgTraitSet requiredOutputTraits, AlgNode alg ) throws AlgConversionException;

    /**
     * Resets this {@code Planner} to be used with a new query. This should be called between each new query.
     */
    void reset();

    /**
     * Releases all internal resources utilized while this {@code Planner} exists.  Once called, this Planner object is no longer valid.
     */
    @Override
    void close();

    AlgTraitSet getEmptyTraitSet();

}

