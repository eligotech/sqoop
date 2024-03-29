/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.sqoop.manager;

import java.sql.Timestamp;

import com.cloudera.sqoop.SqoopOptions;
import com.cloudera.sqoop.hbase.HBaseUtil;
import com.cloudera.sqoop.mapreduce.DataDrivenImportJob;
import com.cloudera.sqoop.mapreduce.HBaseImportJob;
import com.cloudera.sqoop.mapreduce.ImportJobBase;
import com.cloudera.sqoop.mapreduce.JdbcExportJob;
import com.cloudera.sqoop.mapreduce.JdbcUpdateExportJob;
import com.cloudera.sqoop.mapreduce.db.DataDrivenDBInputFormat;
import com.cloudera.sqoop.util.ExportException;
import com.cloudera.sqoop.util.ImportException;
import com.cloudera.sqoop.util.ResultSetPrinter;

import java.io.IOException;
import java.io.PrintWriter;
import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.Statement;
import java.sql.SQLException;
import java.sql.Types;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.util.StringUtils;
import org.apache.sqoop.util.SqlTypeMap;

/**
 * ConnManager implementation for generic SQL-compliant database.
 * This is an abstract class; it requires a database-specific
 * ConnManager implementation to actually create the connection.
 */
public abstract class SqlManager
    extends com.cloudera.sqoop.manager.ConnManager {

  public static final Log LOG = LogFactory.getLog(SqlManager.class.getName());

  /** Substring that must appear in free-form queries submitted by users.
   * This is the string '$CONDITIONS'.
   */
  public static final String SUBSTITUTE_TOKEN =
      DataDrivenDBInputFormat.SUBSTITUTE_TOKEN;

  protected static final int DEFAULT_FETCH_SIZE = 1000;

  protected SqoopOptions options;
  private Statement lastStatement;

  /**
   * Constructs the SqlManager.
   * @param opts the SqoopOptions describing the user's requested action.
   */
  public SqlManager(final SqoopOptions opts) {
    this.options = opts;
    initOptionDefaults();
  }

  /**
   * Sets default values for values that were not provided by the user.
   * Only options with database-specific defaults should be configured here.
   */
  protected void initOptionDefaults() {
    if (options.getFetchSize() == null) {
      LOG.info("Using default fetchSize of " + DEFAULT_FETCH_SIZE);
      options.setFetchSize(DEFAULT_FETCH_SIZE);
    }
  }

  /**
   * @return the SQL query to use in getColumnNames() in case this logic must
   * be tuned per-database, but the main extraction loop is still inheritable.
   */
  protected String getColNamesQuery(String tableName) {
    // adding where clause to prevent loading a big table
    return "SELECT t.* FROM " + escapeTableName(tableName) + " AS t WHERE 1=0";
  }

  @Override
  /** {@inheritDoc} */
  public String[] getColumnNames(String tableName) {
    String stmt = getColNamesQuery(tableName);
    return getColumnNamesForRawQuery(stmt);
  }

  @Override
  /** {@inheritDoc} */
  public String [] getColumnNamesForQuery(String query) {
    String rawQuery = query.replace(SUBSTITUTE_TOKEN, " (1 = 0) ");
    return getColumnNamesForRawQuery(rawQuery);
  }

  /**
   * Get column names for a query statement that we do not modify further.
   */
  public String[] getColumnNamesForRawQuery(String stmt) {
    ResultSet results;
    try {
      results = execute(stmt);
    } catch (SQLException sqlE) {
      LOG.error("Error executing statement: " + sqlE.toString(), sqlE);
      release();
      return null;
    }

    try {
      int cols = results.getMetaData().getColumnCount();
      ArrayList<String> columns = new ArrayList<String>();
      ResultSetMetaData metadata = results.getMetaData();
      for (int i = 1; i < cols + 1; i++) {
        String colName = metadata.getColumnName(i);
        if (colName == null || colName.equals("")) {
          colName = metadata.getColumnLabel(i);
          if (null == colName) {
            colName = "_RESULT_" + i;
          }
        }
        columns.add(colName);
      }
      return columns.toArray(new String[0]);
    } catch (SQLException sqlException) {
      LOG.error("Error reading from database: "
          + sqlException.toString(), sqlException);
      return null;
    } finally {
      try {
        results.close();
        getConnection().commit();
      } catch (SQLException sqlE) {
        LOG.warn("SQLException closing ResultSet: " + sqlE.toString(), sqlE);
      }

      release();
    }
  }

  /**
   * @return the SQL query to use in getColumnTypes() in case this logic must
   * be tuned per-database, but the main extraction loop is still inheritable.
   */
  protected String getColTypesQuery(String tableName) {
    return getColNamesQuery(tableName);
  }

  @Override
  public Map<String, Integer> getColumnTypes(String tableName) {
    String stmt = getColTypesQuery(tableName);
    return getColumnTypesForRawQuery(stmt);
  }

  @Override
  public Map<String, Integer> getColumnTypesForQuery(String query) {
    // Manipulate the query to return immediately, with zero rows.
    String rawQuery = query.replace(SUBSTITUTE_TOKEN, " (1 = 0) ");
    return getColumnTypesForRawQuery(rawQuery);
  }

  /**
   * Get column types for a query statement that we do not modify further.
   */
  protected Map<String, Integer> getColumnTypesForRawQuery(String stmt) {
    ResultSet results;
    try {
      results = execute(stmt);
    } catch (SQLException sqlE) {
      LOG.error("Error executing statement: " + sqlE.toString(), sqlE);
      release();
      return null;
    }

    try {
      Map<String, Integer> colTypes = new SqlTypeMap<String, Integer>();

      int cols = results.getMetaData().getColumnCount();
      ResultSetMetaData metadata = results.getMetaData();
      for (int i = 1; i < cols + 1; i++) {
        int typeId = metadata.getColumnType(i);
        // If we have an unsigned int we need to make extra room by
        // plopping it into a bigint
        if (typeId == Types.INTEGER &&  !metadata.isSigned(i)){
            typeId = Types.BIGINT;
        }

        String colName = metadata.getColumnName(i);
        if (colName == null || colName.equals("")) {
          colName = metadata.getColumnLabel(i);
        }

        colTypes.put(colName, Integer.valueOf(typeId));
      }

      return colTypes;
    } catch (SQLException sqlException) {
      LOG.error("Error reading from database: " + sqlException.toString());
      return null;
    } finally {
      try {
        results.close();
        getConnection().commit();
      } catch (SQLException sqlE) {
        LOG.warn("SQLException closing ResultSet: " + sqlE.toString());
      }

      release();
    }
  }

  @Override
  public Map<String, String> getColumnTypeNamesForTable(String tableName) {
    String stmt = getColTypesQuery(tableName);
    return getColumnTypeNamesForRawQuery(stmt);
  }

  @Override
  public Map<String, String> getColumnTypeNamesForQuery(String query) {
    // Manipulate the query to return immediately, with zero rows.
    String rawQuery = query.replace(SUBSTITUTE_TOKEN, " (1 = 0) ");
    return getColumnTypeNamesForRawQuery(rawQuery);
  }

  protected Map<String, String> getColumnTypeNamesForRawQuery(String stmt) {
    ResultSet results;
    try {
      results = execute(stmt);
    } catch (SQLException sqlE) {
      LOG.error("Error executing statement: " + sqlE.toString(), sqlE);
      release();
      return null;
    }

    try {
      Map<String, String> colTypeNames = new HashMap<String, String>();

      int cols = results.getMetaData().getColumnCount();
      ResultSetMetaData metadata = results.getMetaData();
      for (int i = 1; i < cols + 1; i++) {
        String colTypeName = metadata.getColumnTypeName(i);

        String colName = metadata.getColumnName(i);
        if (colName == null || colName.equals("")) {
          colName = metadata.getColumnLabel(i);
        }

        colTypeNames.put(colName, colTypeName);
      }

      return colTypeNames;
    } catch (SQLException sqlException) {
      LOG.error("Error reading from database: " + sqlException.toString());
      return null;
    } finally {
      try {
        results.close();
        getConnection().commit();
      } catch (SQLException sqlE) {
        LOG.warn("SQLException closing ResultSet: " + sqlE.toString());
      }

      release();
    }
  }

  @Override
  public ResultSet readTable(String tableName, String[] columns)
      throws SQLException {
    if (columns == null) {
      columns = getColumnNames(tableName);
    }

    StringBuilder sb = new StringBuilder();
    sb.append("SELECT ");
    boolean first = true;
    for (String col : columns) {
      if (!first) {
        sb.append(", ");
      }
      sb.append(escapeColName(col));
      first = false;
    }
    sb.append(" FROM ");
    sb.append(escapeTableName(tableName));
    sb.append(" AS ");   // needed for hsqldb; doesn't hurt anyone else.
    sb.append(escapeTableName(tableName));

    String sqlCmd = sb.toString();
    LOG.debug("Reading table with command: " + sqlCmd);
    return execute(sqlCmd);
  }

  @Override
  public String[] listDatabases() {
    // TODO(aaron): Implement this!
    LOG.error("Generic SqlManager.listDatabases() not implemented.");
    return null;
  }

  @Override
  public String[] listTables() {
    ResultSet results = null;
    String [] tableTypes = {"TABLE"};
    try {
      try {
        DatabaseMetaData metaData = this.getConnection().getMetaData();
        results = metaData.getTables(null, null, null, tableTypes);
      } catch (SQLException sqlException) {
        LOG.error("Error reading database metadata: "
            + sqlException.toString());
        return null;
      }

      if (null == results) {
        return null;
      }

      try {
        ArrayList<String> tables = new ArrayList<String>();
        while (results.next()) {
          String tableName = results.getString("TABLE_NAME");
          tables.add(tableName);
        }

        return tables.toArray(new String[0]);
      } catch (SQLException sqlException) {
        LOG.error("Error reading from database: " + sqlException.toString());
        return null;
      }
    } finally {
      if (null != results) {
        try {
          results.close();
          getConnection().commit();
        } catch (SQLException sqlE) {
          LOG.warn("Exception closing ResultSet: " + sqlE.toString());
        }
      }
    }
  }

  @Override
  public String getPrimaryKey(String tableName) {
    try {
      DatabaseMetaData metaData = this.getConnection().getMetaData();
      ResultSet results = metaData.getPrimaryKeys(null, null, tableName);
      if (null == results) {
        return null;
      }

      try {
        if (results.next()) {
          return results.getString("COLUMN_NAME");
        } else {
          return null;
        }
      } finally {
        results.close();
        getConnection().commit();
      }
    } catch (SQLException sqlException) {
      LOG.error("Error reading primary key metadata: "
          + sqlException.toString());
      return null;
    }
  }

  /**
   * Retrieve the actual connection from the outer ConnManager.
   */
  public abstract Connection getConnection() throws SQLException;

  /**
   * Determine what column to use to split the table.
   * @param opts the SqoopOptions controlling this import.
   * @param tableName the table to import.
   * @return the splitting column, if one is set or inferrable, or null
   * otherwise.
   */
  protected String getSplitColumn(SqoopOptions opts, String tableName) {
    String splitCol = opts.getSplitByCol();
    if (null == splitCol && null != tableName) {
      // If the user didn't specify a splitting column, try to infer one.
      splitCol = getPrimaryKey(tableName);
    }
		// LOG.info("splitCol: " + splitCol);
    return splitCol;
  }

  /**
   * Offers the ConnManager an opportunity to validate that the
   * options specified in the ImportJobContext are valid.
   * @throws ImportException if the import is misconfigured.
   */
  protected void checkTableImportOptions(
          com.cloudera.sqoop.manager.ImportJobContext context)
      throws IOException, ImportException {
    String tableName = context.getTableName();
    SqoopOptions opts = context.getOptions();

    // Default implementation: check that the split column is set
    // correctly.
    String splitCol = getSplitColumn(opts, tableName);
		// LOG.info("mappers: "+ opts.getNumMappers());
    if ((null == splitCol || "" == splitCol) && opts.getNumMappers() > 1) {
				opts.setNumMappers(1);
    }
  }

  /**
   * Default implementation of importTable() is to launch a MapReduce job
   * via DataDrivenImportJob to read the table with DataDrivenDBInputFormat.
   */
  public void importTable(com.cloudera.sqoop.manager.ImportJobContext context)
      throws IOException, ImportException {
    String tableName = context.getTableName();
    String jarFile = context.getJarFile();
    SqoopOptions opts = context.getOptions();

    context.setConnManager(this);

    ImportJobBase importer;
    if (opts.getHBaseTable() != null) {
      // Import to HBase.
      if (!HBaseUtil.isHBaseJarPresent()) {
        throw new ImportException("HBase jars are not present in "
            + "classpath, cannot import to HBase!");
      }
      importer = new HBaseImportJob(opts, context);
    } else {
      // Import to HDFS.
      importer = new DataDrivenImportJob(opts, context.getInputFormat(),
              context);
    }

    checkTableImportOptions(context);

    String splitCol = getSplitColumn(opts, tableName);
    importer.runImport(tableName, jarFile, splitCol, opts.getConf());
  }

  /**
   * Default implementation of importQuery() is to launch a MapReduce job
   * via DataDrivenImportJob to read the table with DataDrivenDBInputFormat,
   * using its free-form query importer.
   */
  public void importQuery(com.cloudera.sqoop.manager.ImportJobContext context)
      throws IOException, ImportException {
    String jarFile = context.getJarFile();
    SqoopOptions opts = context.getOptions();

    context.setConnManager(this);

    ImportJobBase importer;
    if (opts.getHBaseTable() != null) {
      // Import to HBase.
      if (!HBaseUtil.isHBaseJarPresent()) {
        throw new ImportException("HBase jars are not present in classpath,"
            + " cannot import to HBase!");
      }
      importer = new HBaseImportJob(opts, context);
    } else {
      // Import to HDFS.
      importer = new DataDrivenImportJob(opts, context.getInputFormat(),
          context);
    }

    String splitCol = getSplitColumn(opts, null);
    if (splitCol == null) {
      String boundaryQuery = opts.getBoundaryQuery();
      if (opts.getNumMappers() > 1) {
        // Can't infer a primary key.
        // throw new ImportException("A split-by column must be specified for "
        //     + "parallel free-form query imports. Please specify one with "
        //     + "--split-by or perform a sequential import with '-m 1'.");
					opts.setNumMappers(1);
      } else if (boundaryQuery != null && !boundaryQuery.isEmpty()) {
        // Query import with boundary query and no split column specified
        throw new ImportException("Using a boundary query for a query based "
            + "import requires specifying the split by column as well. Please "
            + "specify a column name using --split-by and try again.");
      }
    }

    importer.runImport(null, jarFile, splitCol, opts.getConf());
  }

  /**
   * Executes an arbitrary SQL statement.
   * @param stmt The SQL statement to execute
   * @param fetchSize Overrides default or parameterized fetch size
   * @return A ResultSet encapsulating the results or null on error
   */
  protected ResultSet execute(String stmt, Integer fetchSize, Object... args)
      throws SQLException {
    // Release any previously-open statement.
    release();

    PreparedStatement statement = null;
    statement = this.getConnection().prepareStatement(stmt,
        ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_READ_ONLY);
    if (fetchSize != null) {
      LOG.debug("Using fetchSize for next query: " + fetchSize);
      statement.setFetchSize(fetchSize);
    }
    this.lastStatement = statement;
    if (null != args) {
      for (int i = 0; i < args.length; i++) {
        statement.setObject(i + 1, args[i]);
      }
    }

    LOG.info("Executing SQL statement: " + stmt);
    return statement.executeQuery();
  }

  /**
   * Executes an arbitrary SQL Statement.
   * @param stmt The SQL statement to execute
   * @return A ResultSet encapsulating the results or null on error
   */
  protected ResultSet execute(String stmt, Object... args) throws SQLException {
    return execute(stmt, options.getFetchSize(), args);
  }

  public void close() throws SQLException {
    release();
  }

  /**
   * Prints the contents of a ResultSet to the specified PrintWriter.
   * The ResultSet is closed at the end of this method.
   * @param results the ResultSet to print.
   * @param pw the location to print the data to.
   */
  protected void formatAndPrintResultSet(ResultSet results, PrintWriter pw) {
    try {
      try {
        int cols = results.getMetaData().getColumnCount();
        pw.println("Got " + cols + " columns back");
        if (cols > 0) {
          ResultSetMetaData rsmd = results.getMetaData();
          String schema = rsmd.getSchemaName(1);
          String table = rsmd.getTableName(1);
          if (null != schema) {
            pw.println("Schema: " + schema);
          }

          if (null != table) {
            pw.println("Table: " + table);
          }
        }
      } catch (SQLException sqlE) {
        LOG.error("SQLException reading result metadata: " + sqlE.toString());
      }

      try {
        new ResultSetPrinter().printResultSet(pw, results);
      } catch (IOException ioe) {
        LOG.error("IOException writing results: " + ioe.toString());
        return;
      }
    } finally {
      try {
        results.close();
        getConnection().commit();
      } catch (SQLException sqlE) {
        LOG.warn("SQLException closing ResultSet: " + sqlE.toString());
      }

      release();
    }
  }

  /**
   * Poor man's SQL query interface; used for debugging.
   * @param s the SQL statement to execute.
   */
  public void execAndPrint(String s) {
    ResultSet results = null;
    try {
      results = execute(s);
    } catch (SQLException sqlE) {
      LOG.error("Error executing statement: "
          + StringUtils.stringifyException(sqlE));
      release();
      return;
    }

    PrintWriter pw = new PrintWriter(System.out, true);
    try {
      formatAndPrintResultSet(results, pw);
    } finally {
      pw.close();
    }
  }

  /**
   * Create a connection to the database; usually used only from within
   * getConnection(), which enforces a singleton guarantee around the
   * Connection object.
   */
  protected Connection makeConnection() throws SQLException {

    Connection connection;
    String driverClass = getDriverClass();

    try {
      Class.forName(driverClass);
    } catch (ClassNotFoundException cnfe) {
      throw new RuntimeException("Could not load db driver class: "
          + driverClass);
    }

    String username = options.getUsername();
    String password = options.getPassword();
    String connectString = options.getConnectString();
    Properties connectionParams = options.getConnectionParams();
    if (connectionParams != null && connectionParams.size() > 0) {
      LOG.debug("User specified connection params. "
              + "Using properties specific API for making connection.");

      Properties props = new Properties();
      if (username != null) {
        props.put("user", username);
      }

      if (password != null) {
        props.put("password", password);
      }

      props.putAll(connectionParams);
      connection = DriverManager.getConnection(connectString, props);
    } else {
      LOG.debug("No connection paramenters specified. "
              + "Using regular API for making connection.");
      if (username == null) {
        connection = DriverManager.getConnection(connectString);
      } else {
        connection = DriverManager.getConnection(
                        connectString, username, password);
      }
    }

    // We only use this for metadata queries. Loosest semantics are okay.
    connection.setTransactionIsolation(getMetadataIsolationLevel());
    connection.setAutoCommit(false);

    return connection;
  }

  /**
   * @return the transaction isolation level to use for metadata queries
   * (queries executed by the ConnManager itself).
   */
  protected int getMetadataIsolationLevel() {
    return Connection.TRANSACTION_READ_COMMITTED;
  }

  /**
   * Export data stored in HDFS into a table in a database.
   */
  public void exportTable(com.cloudera.sqoop.manager.ExportJobContext context)
      throws IOException, ExportException {
    context.setConnManager(this);
    JdbcExportJob exportJob = new JdbcExportJob(context);
    exportJob.runExport();
  }

  public void release() {
    if (null != this.lastStatement) {
      try {
        this.lastStatement.close();
      } catch (SQLException e) {
        LOG.warn("Exception closing executed Statement: " + e);
      }

      this.lastStatement = null;
    }
  }

  @Override
  /**
   * {@inheritDoc}
   */
  public void updateTable(
          com.cloudera.sqoop.manager.ExportJobContext context)
      throws IOException, ExportException {
    context.setConnManager(this);
    JdbcUpdateExportJob exportJob = new JdbcUpdateExportJob(context);
    exportJob.runExport();
  }

  /**
   * @return a SQL query to retrieve the current timestamp from the db.
   */
  protected String getCurTimestampQuery() {
    return "SELECT CURRENT_TIMESTAMP()";
  }

  @Override
  /**
   * {@inheritDoc}
   */
  public Timestamp getCurrentDbTimestamp() {
    release(); // Release any previous ResultSet.

    Statement s = null;
    ResultSet rs = null;
    try {
      Connection c = getConnection();
      s = c.createStatement();
      rs = s.executeQuery(getCurTimestampQuery());
      if (rs == null || !rs.next()) {
        return null; // empty ResultSet.
      }

      return rs.getTimestamp(1);
    } catch (SQLException sqlE) {
      LOG.warn("SQL exception accessing current timestamp: " + sqlE);
      return null;
    } finally {
      try {
        if (null != rs) {
          rs.close();
        }
      } catch (SQLException sqlE) {
        LOG.warn("SQL Exception closing resultset: " + sqlE);
      }

      try {
        if (null != s) {
          s.close();
        }
      } catch (SQLException sqlE) {
        LOG.warn("SQL Exception closing statement: " + sqlE);
      }
    }
  }

  @Override
  public long getTableRowCount(String tableName) throws SQLException {
    release(); // Release any previous ResultSet
    long result = -1;
    String countQuery = "SELECT COUNT(*) FROM " + tableName;
    Statement stmt = null;
    ResultSet rset = null;
    try {
      Connection conn = getConnection();
      stmt = conn.createStatement();
      rset = stmt.executeQuery(countQuery);
      rset.next();
      result = rset.getLong(1);
    } catch (SQLException ex) {
      LOG.error("Unable to query count * for table " + tableName, ex);
      throw ex;
    } finally {
      if (rset != null) {
        try {
          rset.close();
        } catch (SQLException ex) {
          LOG.error("Unable to close result set", ex);
        }
      }
      if (stmt != null) {
        try {
          stmt.close();
        } catch (SQLException ex) {
          LOG.error("Unable to close statement", ex);
        }
      }
    }
    return result;
  }

  @Override
  public void deleteAllRecords(String tableName) throws SQLException {
    release(); // Release any previous ResultSet
    String deleteQuery = "DELETE FROM " + tableName;
    Statement stmt = null;
    try {
      Connection conn = getConnection();
      stmt = conn.createStatement();
      int updateCount = stmt.executeUpdate(deleteQuery);
      conn.commit();
      LOG.info("Deleted " + updateCount + " records from " + tableName);
    } catch (SQLException ex) {
      LOG.error("Unable to execute delete query: "  + deleteQuery, ex);
      throw ex;
    } finally {
      if (stmt != null) {
        try {
          stmt.close();
        } catch (SQLException ex) {
          LOG.error("Unable to close statement", ex);
        }
      }
    }
  }

  @Override
  public void migrateData(String fromTable, String toTable)
    throws SQLException {
    release(); // Release any previous ResultSet
    String updateQuery = "INSERT INTO " + toTable
          + " ( SELECT * FROM " + fromTable + " )";

    String deleteQuery = "DELETE FROM " + fromTable;
    Statement stmt = null;
    try {
      Connection conn = getConnection();
      stmt = conn.createStatement();

      // Insert data from the fromTable to the toTable
      int updateCount = stmt.executeUpdate(updateQuery);
      LOG.info("Migrated " + updateCount + " records from " + fromTable
                  + " to " + toTable);

      // Delete the records from the fromTable
      int deleteCount = stmt.executeUpdate(deleteQuery);

      // If the counts do not match, fail the transaction
      if (updateCount != deleteCount) {
        conn.rollback();
        throw new RuntimeException("Inconsistent record counts");
      }
      conn.commit();
    } catch (SQLException ex) {
      LOG.error("Unable to migrate data from "
          + fromTable + " to " + toTable, ex);
      throw ex;
    } finally {
      if (stmt != null) {
        try {
          stmt.close();
        } catch (SQLException ex) {
          LOG.error("Unable to close statement", ex);
        }
      }
    }
  }

  public String getInputBoundsQuery(String splitByCol, String sanitizedQuery) {
    return options.getBoundaryQuery();
  }
}
