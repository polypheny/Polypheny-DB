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

package org.polypheny.db.sql.language;


import java.util.ArrayList;
import java.util.Collection;
import java.util.Iterator;
import java.util.List;
import lombok.Getter;
import org.polypheny.db.algebra.constant.Kind;
import org.polypheny.db.algebra.operators.OperatorName;
import org.polypheny.db.languages.OperatorRegistry;
import org.polypheny.db.languages.ParserPos;
import org.polypheny.db.nodes.Node;
import org.polypheny.db.nodes.NodeList;
import org.polypheny.db.nodes.NodeVisitor;
import org.polypheny.db.sql.language.validate.SqlValidator;
import org.polypheny.db.sql.language.validate.SqlValidatorScope;
import org.polypheny.db.util.Litmus;


/**
 * A <code>SqlNodeList</code> is a list of {@link SqlNode}s. It is also a {@link SqlNode}, so may appear in a parse tree.
 */
public class SqlNodeList extends SqlNode implements NodeList {

    /**
     * An immutable, empty SqlNodeList.
     */
    public static final SqlNodeList EMPTY =
            new SqlNodeList( ParserPos.ZERO ) {
                @Override
                public void add( Node node ) {
                    throw new UnsupportedOperationException();
                }
            };


    private final List<Node> list;
    @Getter
    private final List<SqlNode> sqlList;


    /**
     * Creates an empty <code>SqlNodeList</code>.
     */
    public SqlNodeList( ParserPos pos ) {
        super( pos );
        list = new ArrayList<>();
        sqlList = new ArrayList<>();
    }


    /**
     * Creates a <code>SqlNodeList</code> containing the nodes in <code>list</code>. The list is copied, but the nodes in it are not.
     */
    public SqlNodeList( Collection<? extends SqlNode> collection, ParserPos pos ) {
        super( pos );
        list = new ArrayList<>( collection );
        sqlList = new ArrayList<>( collection.stream().map( e -> (SqlNode) e ).toList() );
    }


    // implement Iterable<SqlNode>
    @Override
    public Iterator<Node> iterator() {
        return list.iterator();
    }


    @Override
    public List<Node> getList() {
        return list;
    }


    @Override
    public void add( Node node ) {
        list.add( node );
        sqlList.add( (SqlNode) node );
    }


    @Override
    public SqlNodeList clone( ParserPos pos ) {
        return new SqlNodeList( sqlList, pos );
    }


    @Override
    public Node get( int n ) {
        return list.get( n );
    }


    @Override
    public Node set( int n, Node node ) {
        sqlList.set( n, (SqlNode) node );
        return list.set( n, node );
    }


    @Override
    public int size() {
        return list.size();
    }


    @Override
    public void unparse( SqlWriter writer, int leftPrec, int rightPrec ) {
        final SqlWriter.Frame frame =
                ((leftPrec > 0) || (rightPrec > 0))
                        ? writer.startList( "(", ")" )
                        : writer.startList( "", "" );
        commaList( writer );
        writer.endList( frame );
    }


    void commaList( SqlWriter writer ) {
        // The precedence of the comma operator if low but not zero. For instance, this ensures parentheses in select x, (select * from foo order by z), y from t
        for ( Node node : list ) {
            writer.sep( "," );
            ((SqlNode) node).unparse( writer, 2, 3 );
        }
    }


    void andOrList( SqlWriter writer, Kind sepKind ) {
        SqlBinaryOperator sepOp =
                sepKind == Kind.AND
                        ? OperatorRegistry.get( OperatorName.AND, SqlBinaryOperator.class )
                        : OperatorRegistry.get( OperatorName.OR, SqlBinaryOperator.class );
        for ( int i = 0; i < list.size(); i++ ) {
            Node node = list.get( i );
            writer.sep( sepKind.name(), false );

            // The precedence pulling on the LHS of a node is the right-precedence of the separator operator, except at the start of the list; similarly for the RHS of a node. If the operator
            // has left precedence 4 and right precedence 5, the precedences in a 3-node list will look as follows:
            // 0 <- node1 -> 4
            // 5 <- node2 -> 4
            // 5 <- node3 -> 0
            int lprec = (i == 0) ? 0 : sepOp.getRightPrec();
            int rprec = (i == (list.size() - 1)) ? 0 : sepOp.getLeftPrec();
            ((SqlNode) node).unparse( writer, lprec, rprec );
        }
    }


    @Override
    public void validate( SqlValidator validator, SqlValidatorScope scope ) {
        for ( Node child : list ) {
            ((SqlNode) child).validate( validator, scope );
        }
    }


    @Override
    public <R> R accept( NodeVisitor<R> visitor ) {
        return visitor.visit( this );
    }


    @Override
    public boolean equalsDeep( Node node, Litmus litmus ) {
        if ( !(node instanceof SqlNodeList that) ) {
            return litmus.fail( "{} != {}", this, node );
        }
        if ( this.size() != that.size() ) {
            return litmus.fail( "{} != {}", this, node );
        }
        for ( int i = 0; i < list.size(); i++ ) {
            Node thisChild = list.get( i );
            final Node thatChild = that.list.get( i );
            if ( !thisChild.equalsDeep( thatChild, litmus ) ) {
                return litmus.fail( null );
            }
        }
        return litmus.succeed();
    }


    @Override
    public Node[] toArray() {
        return list.toArray( new Node[0] );
    }


    public static boolean isEmptyList( final Node node ) {
        if ( node instanceof SqlNodeList ) {
            return 0 == ((SqlNodeList) node).size();
        }
        return false;
    }


    public static SqlNodeList of( SqlNode node1 ) {
        SqlNodeList list = new SqlNodeList( ParserPos.ZERO );
        list.add( node1 );
        return list;
    }


    public static SqlNodeList of( SqlNode node1, SqlNode node2 ) {
        SqlNodeList list = new SqlNodeList( ParserPos.ZERO );
        list.add( node1 );
        list.add( node2 );
        return list;
    }


    public static SqlNodeList of( SqlNode node1, SqlNode node2, SqlNode... nodes ) {
        SqlNodeList list = new SqlNodeList( ParserPos.ZERO );
        list.add( node1 );
        list.add( node2 );
        for ( SqlNode node : nodes ) {
            list.add( node );
        }
        return list;
    }


    @Override
    public void validateExpr( SqlValidator validator, SqlValidatorScope scope ) {
        // While a SqlNodeList is not always a valid expression, this implementation makes that assumption. It just validates the members of the list.
        //
        // One example where this is valid is the IN operator. The expression
        //
        //    empno IN (10, 20)
        //
        // results in a call with operands
        //
        //    {  SqlIdentifier({"empno"}), SqlNodeList(SqlLiteral(10), SqlLiteral(20))  }

        for ( SqlNode node : sqlList ) {
            node.validateExpr( validator, scope );
        }
    }

}

