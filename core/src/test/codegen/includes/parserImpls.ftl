
SqlAlter SqlUploadJarNode(Span s, String scope) :
{
    SqlNode jarPath;
    final List<SqlNode> jarPathsList;
}
{
    <UPLOAD> <JAR>
    jarPath = StringLiteral() {
        jarPathsList = startList(jarPath);
    }
    (
        <COMMA> jarPath = StringLiteral() {
            jarPathsList.add(jarPath);
        }
    )*
    {
        return new SqlUploadJarNode(s.end(this), scope, jarPathsList);
    }
}

SqlCreate SqlCreateTable(Span s, boolean replace) :
{
    final SqlIdentifier id;
    final SqlNodeList columnList;
}
{
    <TABLE> id = CompoundIdentifier() columnList = ExtendList() {
        return new SqlCreateTable(s.end(columnList), id, columnList);
    }
}

SqlNode SqlDescribeSpacePower() :
{
}
{
    <DESCRIBE> <SPACE> <POWER> {
        return null;
    }
}
