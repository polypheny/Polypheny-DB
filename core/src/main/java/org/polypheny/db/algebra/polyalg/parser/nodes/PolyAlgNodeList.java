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

package org.polypheny.db.algebra.polyalg.parser.nodes;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Iterator;
import java.util.List;
import lombok.Getter;
import org.jetbrains.annotations.NotNull;
import org.polypheny.db.languages.ParserPos;
import org.polypheny.db.nodes.Node;
import org.polypheny.db.nodes.NodeList;

public class PolyAlgNodeList extends PolyAlgNode implements NodeList {

    private final List<Node> list;
    @Getter
    private final List<PolyAlgNode> polyAlgList;


    public PolyAlgNodeList( ParserPos pos ) {
        super( pos );
        list = new ArrayList<>();
        polyAlgList = new ArrayList<>();
    }


    public PolyAlgNodeList( Collection<? extends PolyAlgNode> collection, ParserPos pos ) {
        super( pos );
        list = new ArrayList<>( collection );
        polyAlgList = new ArrayList<>( collection.stream().map( e -> (PolyAlgNode) e ).toList() );
    }


    @Override
    public List<Node> getList() {
        return list;
    }


    @Override
    public void add( Node node ) {
        list.add( node );
        polyAlgList.add( (PolyAlgNode) node );

    }


    @Override
    public Node get( int n ) {
        return list.get( n );
    }


    @Override
    public Node set( int n, Node node ) {
        polyAlgList.set( n, (PolyAlgNode) node );
        return list.set( n, node );
    }


    @Override
    public int size() {
        return list.size();
    }


    @Override
    public Node[] toArray() {
        return list.toArray( new Node[0]);
    }


    @NotNull
    @Override
    public Iterator<Node> iterator() {
        return list.iterator();
    }

}
