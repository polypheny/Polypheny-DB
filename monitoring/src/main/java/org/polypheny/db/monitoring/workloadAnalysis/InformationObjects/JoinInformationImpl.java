/*
 * Copyright 2019-2022 The Polypheny Project
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

package org.polypheny.db.monitoring.workloadAnalysis.InformationObjects;

import java.util.ArrayList;
import java.util.List;
import lombok.Getter;
import org.polypheny.db.adaptiveness.JoinInformation;
import org.polypheny.db.util.Pair;

@Getter
public class JoinInformationImpl implements JoinInformation {

    private final List<Pair<Long, Long>> jointTableIds = new ArrayList<>();
    private final List<Pair<Long, Long>> jointColumnIds = new ArrayList<>();
    private int joinCount;

    public JoinInformationImpl(  ) {
        this.joinCount = 0;
    }

    @Override
    public void updateJoinInformation( Long tableIdLeft, Long tableIdRight ){
        this.joinCount += 1;
        this.jointTableIds.add( new Pair<>( tableIdLeft, tableIdRight ) );
    }


}
