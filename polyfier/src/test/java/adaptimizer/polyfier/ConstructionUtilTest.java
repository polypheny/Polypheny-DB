/*
 * Copyright 2019-2023 The Polypheny Project
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

package adaptimizer.polyfier;


import lombok.extern.slf4j.Slf4j;
import org.polypheny.db.adaptimizer.polyfierconnect.pseudo.ConstructUtil;
import org.polypheny.db.type.PolyType;

import java.util.List;

@Slf4j
public class ConstructionUtilTest {


    public static void main(String[] args) {

        List<PolyType> left = List.of( PolyType.DATE, PolyType.VARCHAR, PolyType.VARCHAR, PolyType.FLOAT, PolyType.INTEGER, PolyType.DOUBLE, PolyType.DOUBLE, PolyType.DOUBLE );
        List<PolyType> right = List.of( PolyType.INTEGER, PolyType.DOUBLE, PolyType.VARCHAR, PolyType.DOUBLE, PolyType.TIME, PolyType.TIME, PolyType.INTEGER, PolyType.FLOAT );

        ConstructUtil.SetOpProblem<PolyType> setOpProblem = new ConstructUtil.SetOpProblem<>( left, right );

        setOpProblem.get().ifPresentOrElse(
                solution -> System.out.println( solution.getProjectionCodeLeft().toString()  + " : " + solution.getProjectionCodeRight().toString() ),
                () -> System.out.println( "No Solution." )
        );


    }



}

