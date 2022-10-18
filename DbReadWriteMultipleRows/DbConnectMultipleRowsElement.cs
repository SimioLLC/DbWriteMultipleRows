using System;
using System.Collections;
using System.Collections.Generic;
using System.Text;
using SimioAPI;
using SimioAPI.Extensions;
using System.Data;
using System.Data.Common;
using System.Globalization;

namespace DbReadWriteMultipleRows
{
    class DBConnectMultipleRowsElementDefinition : IElementDefinition
    {
        #region IElementDefinition Members

        /// <summary>
        /// Property returning the full name for this type of element. The name should contain no spaces. 
        /// </summary>
        public string Name
        {
            get { return "DbConnectMultipleRows"; }
        }

        /// <summary>
        /// Property returning a short description of what the element does.  
        /// </summary>
        public string Description
        {
            get { return "Used with DbReadMultipleRows, DbWriteMultipleRows, and DbExecuteMultipleRows steps."; }
        }

        /// <summary>
        /// Property returning an icon to display for the element in the UI. 
        /// </summary>
        public System.Drawing.Image Icon
        {
            get { return null; }
        }

        /// <summary>
        /// Property returning a unique static GUID for the element.  
        /// </summary>
        public Guid UniqueID
        {
            get { return MY_ID; }
        }
        // We need to use this ID in the element reference property of the Read/Write steps, so we make it public
        public static readonly Guid MY_ID = new Guid("{e2ccf4e2-988a-48d8-a0ec-f9ef9cbfc20c}");

        /// <summary>
        /// Method called that defines the property, state, and event schema for the element.
        /// </summary>
        public void DefineSchema(IElementSchema schema)
        {
            IPropertyDefinition pd = schema.PropertyDefinitions.AddStringProperty("ConnectionString", "Server=localhost\\SQLExpress;Database=Test;Integrated Security=true");
            pd.DisplayName = "Connection String";
            pd.Description = "The connection string used to define the connection to the database.";           
            pd.Required = true;

            IPropertyDefinition pd2 = schema.PropertyDefinitions.AddStringProperty("ProviderName", "SqlClient Data Provider");
            pd2.DisplayName = "Provider Name";
            pd2.Description = "The provider type used to specify what database provider to use to connect to the database.";            
            pd2.Required = true;
        }

        /// <summary>
        /// Method called to add a new instance of this element type to a model. 
        /// Returns an instance of the class implementing the IElement interface.
        /// </summary>
        public IElement CreateElement(IElementData data)
        {
            return new DBConnectMultipleRowsElement(data);
        }

        #endregion
    }

    class DBConnectMultipleRowsElement : IElement
    {
        IElementData _data;
        private DbProviderFactory _db;
        private DbConnection _connection;
        public DBConnectMultipleRowsElement(IElementData data)
        {
            _data = data;
            IPropertyReader connectStringProp = _data.Properties.GetProperty("ConnectionString");
            IPropertyReader providerNameProp = _data.Properties.GetProperty("ProviderName");

            // Get and cache the connect string and connection
            string connectionString = connectStringProp.GetStringValue(_data.ExecutionContext);
            string providerName = providerNameProp.GetStringValue(_data.ExecutionContext);

            DataTable table = DbProviderFactories.GetFactoryClasses();

            // Display each row and column value.
            List<String> providerNames = new List<String>();
            foreach (DataRow row in table.Rows)
            {
                providerNames.Add(row[0].ToString());
                if (row[0].ToString() == providerNameProp.GetStringValue(_data.ExecutionContext))
                {
                    _db = DbProviderFactories.GetFactory(row);
                    break;
                }
            }

            if (_db == null)
            {
                string msg = "Provider not found. Available providers are : " + System.Environment.NewLine + string.Join(System.Environment.NewLine, providerNames.ToArray());
                data.ExecutionContext.ExecutionInformation.ReportError(msg);
            }


            if (_connection == null)
            {
                try
                {
                    _connection = _db.CreateConnection();
                }
                catch(Exception e)
                {
                    string msg = String.Format("Exception trying to create the connection object. Message: '{0}'", e.Message);
                    data.ExecutionContext.ExecutionInformation.ReportError(msg);
                    _connection = null;
                    return;
                }

                var connectionStringPropValue = connectStringProp.GetStringValue(_data.ExecutionContext);
                try
                {
                    _connection.ConnectionString = connectionStringPropValue;
                }
                catch(Exception e)
                {
                    string msg = String.Format("Exception trying to set connection string '{0}'. Message: '{1}'", connectionStringPropValue, e.Message);
                    data.ExecutionContext.ExecutionInformation.ReportError(msg);
                    _connection = null;
                    return;
                }

                try
                {
                    _connection.Open();
                }
                catch(Exception e)
                {
                    string msg = String.Format("Exception trying to open database connection. Message: '{0}'", e.Message);
                    data.ExecutionContext.ExecutionInformation.ReportError(msg);
                    _connection = null;
                    return;
                }
            }
        }

        public string[,] QueryResults(string sqlString)
        {            
            DbDataAdapter dataAdapter = _db.CreateDataAdapter();
            var command = _connection.CreateCommand();
            command.CommandText = sqlString;
            dataAdapter.SelectCommand = command;
            DataTable dataTable = new DataTable();
            dataTable.Locale = System.Globalization.CultureInfo.InvariantCulture;
            dataAdapter.Fill(dataTable);

            string[,] stringArray = new string[dataTable.Rows.Count, dataTable.Columns.Count];

            for (int row = 0; row < dataTable.Rows.Count; ++row)
            {
                for (int col = 0; col < dataTable.Columns.Count; col++)
                {
                    stringArray[row, col] = dataTable.Rows[row][col].ToString();
                }
            }

            return stringArray;
        }

        public int ExecuteResults(string sqlString)
        {
            var command = _connection.CreateCommand();
            command.CommandText = sqlString;
            return command.ExecuteNonQuery();
        }

        public void WriteTable(string tableName, string[,] stringArray, int numOfRows)
        {
            // setup data adapter
            DbDataAdapter dataAdapter = _db.CreateDataAdapter();
            var command = _connection.CreateCommand();
            command.CommandText = "Select * from " + tableName;
            dataAdapter.SelectCommand = command;

            // define command builder
            DbCommandBuilder commandBuilder = _db.CreateCommandBuilder();
            commandBuilder.DataAdapter = dataAdapter;
            dataAdapter.InsertCommand = commandBuilder.GetInsertCommand();

            // define data table
            DataTable dataTable = new DataTable();
            dataTable.Locale = System.Globalization.CultureInfo.InvariantCulture;
            DataTable[] dataTables = { dataTable };
            dataAdapter.Fill(1, 1, dataTables);

            // define data row
            DataRow dataRow = null;

            int numberOfColumns = (int)(stringArray.Length / numOfRows);

            // for each parameter
            for (int i = 0; i < numOfRows; i++)
            {
                for (int j = 0; j < numberOfColumns; j++)
                {
                    if (j == 0)
                    {                       
                        dataRow = dataTable.NewRow();                        
                    }
                    dataRow[j] = stringArray[i, j];
                }
                dataTable.Rows.Add(dataRow);
            }
            dataAdapter.Update(dataTable);
        }

        public string[,] ReadTable(string tableName, int numOfColumns, string[,] whereArray, out string[,] stringArray, out int numOfRows)
        {
            // get wheres
            string wheresNamesConcat = "";
            int numWRows = whereArray.Length / 2;
            for (int i = 0; i < numWRows; i++)
            {
                if (i == 0)
                {
                    wheresNamesConcat = whereArray[i, 0] + " = " + whereArray[i, 1]; 
                }
                else
                {
                    wheresNamesConcat = wheresNamesConcat + " and " + whereArray[i, 0] + " = " + whereArray[i, 1];
                }
            }

            // setup data adapter
            DbDataAdapter dataAdapter = _db.CreateDataAdapter();
            var command = _connection.CreateCommand();

            if (wheresNamesConcat.Length > 0)
            {
                command.CommandText = "Select * from " + tableName + " where " + wheresNamesConcat;
            }
            else
            {
                command.CommandText = "Select * from " + tableName;
            }
            dataAdapter.SelectCommand = command;

            DataTable dataTable = new DataTable();
            dataTable.Locale = System.Globalization.CultureInfo.InvariantCulture;
            dataAdapter.Fill(dataTable);
            numOfRows = dataTable.Rows.Count;
            stringArray = new string[numOfRows, numOfColumns];

            int rowNumber = -1;
            foreach(DataRow dataRow in dataTable.Rows)
            {
                rowNumber++;
                for (int col = 0; col < dataTable.Columns.Count; col++)
                {
                    stringArray[rowNumber,col] = dataRow.ItemArray[col].ToString();
                }
            }         

            return stringArray;
        }
        
        #region IElement Members

        /// <summary>
        /// Method called when the simulation run is initialized.
        /// </summary>
        public void Initialize()
        { 
            // No initialization logic needed, we will open the file on the first read or write request
        }

        /// <summary>
        /// Method called when the simulation run is terminating.
        /// </summary>
        public void Shutdown()
        {
            // On shutdown, we need to make sure to close the DB Connection
            if (_connection != null)
            {
                _connection.Close();
                _connection.Dispose();
                _connection = null;
            }
        }

        #endregion
    }
}
