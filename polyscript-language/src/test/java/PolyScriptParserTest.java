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

import static org.junit.Assert.assertEquals;
import org.junit.Test;
import org.polypheny.db.polyscript.parser.ParseException;
import org.polypheny.db.polyscript.parser.PolyScript;

import java.io.StringReader;
import java.util.List;

public class PolyScriptParserTest {

    @Test
    public void parseMultiLines() throws ParseException {
        List<String> result = new PolyScript(new StringReader("sql(select * from emps1);sql(select * from emps2);")).Start();
        assertEquals(2, result.size());
        String first = result.get(0);
        assertEquals( "select * from emps1", first);
        String second = result.get(1);
        assertEquals( "select * from emps2", second);
    }
}
