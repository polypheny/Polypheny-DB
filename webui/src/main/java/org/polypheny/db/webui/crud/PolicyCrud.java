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

package org.polypheny.db.webui.crud;

import com.google.gson.Gson;
import io.javalin.http.Context;
import lombok.Getter;
import org.polypheny.db.policies.policy.PolicyManager;
import org.polypheny.db.webui.Crud;

public class PolicyCrud {

    @Getter
    private static Crud crud;

    private final PolicyManager policyManager = PolicyManager.getInstance();


    public PolicyCrud( Crud crud ) {
        PolicyCrud.crud = crud;
    }



    public void updatePolicies(final Context ctx, Gson gsonExpose){

        policyManager.updatePolicies();
    }


    public void getPolicies(final Context ctx  ) {
    }

}
