package org.embulk.input.sqlserver;

import java.sql.Connection;
import java.sql.SQLException;

import com.google.common.base.Optional;
import org.embulk.input.jdbc.JdbcInputConnection;

public class SQLServerInputConnection extends JdbcInputConnection {

    public SQLServerInputConnection(Connection connection, String schemaName) throws SQLException
    {
        super(connection, schemaName);
    }

    @Override
    protected void setSearchPath(String schema) throws SQLException
    {
        // NOP
    }

    @Override
    protected String buildTableName(String tableName)
    {
        StringBuilder sb = new StringBuilder();
        if (schemaName != null) {
            sb.append(quoteIdentifierString(schemaName)).append(".");
        }
        sb.append(quoteIdentifierString(tableName));
        return sb.toString();
    }

    @Override
    public String buildSelectQuery(String tableName,
                                   Optional<String> selectExpression, Optional<String> whereCondition,
                                   Optional<String> orderByExpression) throws SQLException
    {
        StringBuilder sb = new StringBuilder();

        sb.append("SELECT ");
        sb.append(selectExpression.or("*"));
        sb.append(" FROM ").append(buildTableName(tableName));
        sb.append("(nolock)");

        if (whereCondition.isPresent()) {
            sb.append(" WHERE ").append(whereCondition.get());
        }

        if (orderByExpression.isPresent()) {
            sb.append(" ORDER BY ").append(orderByExpression.get());
        }

        return sb.toString();
    }

}
