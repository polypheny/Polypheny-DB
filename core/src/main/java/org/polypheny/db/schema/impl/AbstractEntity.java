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

package org.polypheny.db.schema.impl;


import com.google.common.collect.ImmutableList;
import lombok.Getter;
import org.polypheny.db.StatisticsManager;
import org.polypheny.db.nodes.Call;
import org.polypheny.db.nodes.Node;
import org.polypheny.db.schema.Entity;
import org.polypheny.db.schema.Statistic;
import org.polypheny.db.schema.Statistics;
import org.polypheny.db.util.Wrapper;


/**
 * Abstract base class for implementing {@link Entity}.
 *
 * Sub-classes should override {@link #isRolledUp} and {@link Entity#rolledUpColumnValidInsideAgg(String, Call, Node)} if
 * their table can potentially contain rolled up values. This information is used by the validator to check for illegal uses
 * of these columns.
 */
@Getter
public abstract class AbstractEntity implements Entity, Wrapper {

    @Getter
    protected final Long partitionId;
    @Getter
    private final Long id;

    @Getter
    private final Long adapterId;

    protected Statistic statistic = Statistics.UNKNOWN;


    protected AbstractEntity( Long id, Long partitionId, Long adapterId ) {
        this.id = id;
        this.partitionId = partitionId;
        this.adapterId = adapterId;
    }


    @Override
    public boolean isRolledUp( String column ) {
        return false;
    }


    @Override
    public boolean rolledUpColumnValidInsideAgg( String column, Call call, Node parent ) {
        return true;
    }


    @Override
    public Statistic getStatistic() {
        if ( id == null ) {
            return Statistics.UNKNOWN;
        }
        Long rowCount = StatisticsManager.getInstance().tupleCountPerEntity( id );

        if ( rowCount == null ) {
            return Statistics.UNKNOWN;
        } else {
            return Statistics.of( Double.valueOf( rowCount ), ImmutableList.of() );
        }
    }

}

