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

package org.polypheny.db.mql.mql.dql;

import java.util.Map;
import org.bson.BsonValue;
import org.junit.jupiter.api.Test;
import org.polypheny.db.languages.QueryLanguage;
import org.polypheny.db.languages.mql.Mql.Type;
import org.polypheny.db.languages.mql.MqlFind;
import org.polypheny.db.languages.mql.MqlNode;
import org.polypheny.db.mql.mql.MqlTest;


public class MqlFindTest extends MqlTest {

    public String find( String match ) {
        return "db.secrets.find({" + match + "})";
    }


    public String find( String match, String project ) {
        return "db.secrets.find({" + match + "},{" + project + "})";
    }


    private void defaultTests( MqlNode parsed ) {
        assert (parsed.getMqlKind() == Type.FIND);
        assert (parsed.getLanguage() == QueryLanguage.from( "mongo" ));
        assert (parsed.getPrimary().isEmpty());
        assert (parsed.getStores().isEmpty());
    }


    @Test
    public void testEmptyMatch() {
        MqlNode parsed = parse( find( "" ) );
        defaultTests( parsed );
        MqlFind find = (MqlFind) parsed;
        assert (find.getProjection().isEmpty());
        assert (find.getQuery().isEmpty());
    }


    @Test
    public void testSingleMatch() {
        MqlNode parsed = parse( find( "\"key\":\"value\"" ) );
        defaultTests( parsed );
        MqlFind find = (MqlFind) parsed;
        assert (find.getProjection().isEmpty());
        assert (find.getQuery().size() == 1);
        assert (find.getQuery().getFirstKey().equals( "key" ));
        BsonValue value = find.getQuery().get( find.getQuery().getFirstKey() );
        assert (value.isString());
        assert (value.asString().getValue().equals( "value" ));
    }


    @Test
    public void testMultipleMatch() {
        MqlNode parsed = parse( find( "\"key\":\"value\", \"key2\":\"value2\"" ) );
        defaultTests( parsed );
        MqlFind find = (MqlFind) parsed;
        assert (find.getProjection().isEmpty());
        assert (find.getQuery().size() == 2);

        Map<String, BsonValue> values = find.getQuery();
        assert (values.containsKey( "key" ));
        assert (values.get( "key" ).isString() && values.get( "key" ).asString().getValue().equals( "value" ));

        assert (values.containsKey( "key2" ));
        assert (values.get( "key2" ).isString() && values.get( "key2" ).asString().getValue().equals( "value2" ));
    }


    @Test
    public void testSingleProject() {
        MqlNode parsed = parse( find( "", "\"key\":\"value\"" ) );
        defaultTests( parsed );
        MqlFind find = (MqlFind) parsed;
        assert (find.getQuery().isEmpty());
        assert (find.getProjection().size() == 1);
        assert (find.getProjection().getFirstKey().equals( "key" ));
        BsonValue value = find.getProjection().get( find.getProjection().getFirstKey() );
        assert (value.isString());
        assert (value.asString().getValue().equals( "value" ));
    }


    @Test
    public void testMultipleProject() {
        MqlNode parsed = parse( find( "", "\"key\":\"value\", \"key2\":\"value2\"" ) );
        defaultTests( parsed );
        MqlFind find = (MqlFind) parsed;
        assert (find.getQuery().isEmpty());
        assert (find.getProjection().size() == 2);

        Map<String, BsonValue> values = find.getProjection();
        assert (values.containsKey( "key" ));
        assert (values.get( "key" ).isString() && values.get( "key" ).asString().getValue().equals( "value" ));

        assert (values.containsKey( "key2" ));
        assert (values.get( "key2" ).isString() && values.get( "key2" ).asString().getValue().equals( "value2" ));
    }

}
