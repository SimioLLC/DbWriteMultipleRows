using System;
using System.Globalization;
using SimioAPI;
using SimioAPI.Extensions;

namespace DbReadWriteMultipleRows
{
    public class DbWriteMultipleRowsStepDefinition : IStepDefinition
    {
        #region IStepDefinition Members

        /// <summary>
        /// Property returning the full name for this type of step. The name should contain no spaces. 
        /// </summary>
        public string Name
        {
            get { return "DbWriteMultipleRows"; }
        }

        /// <summary>
        /// Property returning a short description of what the step does.  
        /// </summary>
        public string Description
        {
            get { return "The DbWriteMultipleRows step may be used to write data to a database."; }
        }

        /// <summary>
        /// Property returning an icon to display for the step in the UI. 
        /// </summary>
        public System.Drawing.Image Icon
        {
            get { return null; }
        }

        /// <summary>
        /// Property returning a unique static GUID for the step.  
        /// </summary>
        public Guid UniqueID
        {
            get { return MY_ID; }
        }
        static readonly Guid MY_ID = new Guid("{f48abbfd-61b7-4aa3-b04a-2fe234176db9}");

        /// <summary>
        /// Property returning the number of exits out of the step. Can return either 1 or 2. 
        /// </summary>
        public int NumberOfExits
        {
            get { return 1; }
        }

        /// <summary>
        /// Method called that defines the property schema for the step.
        /// </summary>
        public void DefineSchema(IPropertyDefinitions schema)
        {
            IPropertyDefinition pd;

            // Reference to the file to write to
            pd = schema.AddElementProperty("DbConnectMultipleRows", DBConnectMultipleRowsElementDefinition.MY_ID);

            pd = schema.AddTableReferenceProperty("SourceTable");
            pd.DisplayName = "Soruce Table";
            pd.Description = "Simio source table";
            pd.Required = true;

            pd = schema.AddStringProperty("DestinationTable",String.Empty);
            pd.DisplayName = "Destination Table";
            pd.Description = "The database table name where the data is to be written.";
            pd.Required = true;
        }

        /// <summary>
        /// Method called to create a new instance of this step type to place in a process. 
        /// Returns an instance of the class implementing the IStep interface.
        /// </summary>
        public IStep CreateStep(IPropertyReaders properties)
        {
            return new DbWriteMultipleRowsStep(properties);
        }

        #endregion
    }

    class DbWriteMultipleRowsStep : IStep
    {
        IPropertyReaders _props;
        ITableReferencePropertyReader _sourceTableReaderProp;
        IPropertyReader _destinationTableProp;
        IElementProperty _dbconnectMultipleRowsElementProp;
        public DbWriteMultipleRowsStep(IPropertyReaders properties)
        {
            _props = properties;
            _dbconnectMultipleRowsElementProp = (IElementProperty)_props.GetProperty("DbConnectMultipleRows");
            _sourceTableReaderProp = (ITableReferencePropertyReader)_props.GetProperty("SourceTable");
            _destinationTableProp = _props.GetProperty("DestinationTable");
        }

        #region IStep Members

        /// <summary>
        /// Method called when a process token executes the step.
        /// </summary>
        public ExitType Execute(IStepExecutionContext context)
        {
            DBConnectMultipleRowsElement dbconnect = (DBConnectMultipleRowsElement)_dbconnectMultipleRowsElementProp.GetElement(context);
            ITableRuntimeData sourceTable = _sourceTableReaderProp.GetTableReference(context);
            String destinationTableName = _destinationTableProp.GetStringValue(context);

            int numOfRows = sourceTable.GetCount(context);
            int numOfColumns = sourceTable.Table.Columns.Count + sourceTable.Table.StateColumns.Count;    

            object[,] paramsArray = new object[numOfRows, numOfColumns];

            // an array of string values from the repeat group's list of strings
            for (int i = 0; i < numOfRows; i++)
            {
                ITableRuntimeDataRow row = sourceTable.GetRow(i, context);

                for (int j = 0; j < sourceTable.Table.Columns.Count; j++)
                {
                    paramsArray[i, j] = row.Properties.GetProperty(sourceTable.Table.Columns[j].Name).GetStringValue(context);
                }

                for (int j = 0; j < sourceTable.Table.StateColumns.Count; j++)
                {
                    paramsArray[i, j + sourceTable.Table.Columns.Count] = TryReadState(row.States[j]);
                }  
            }

            try
            {
                // for each parameter
                string[,] stringArray = new string[numOfRows, numOfColumns];
                for (int i = 0; i < numOfRows; i++)
                {
                    for (int j = 0; j < numOfColumns; j++)
                    {
                        double doubleValue = paramsArray[i, j] is double ? (double)paramsArray[i, j] : Double.NaN;
                        if (!System.Double.IsNaN(doubleValue))
                        {
                            stringArray[i, j] = (Convert.ToString(doubleValue, CultureInfo.InvariantCulture));
                        }
                        else
                        {
                            DateTime datetimeValue = TryAsDateTime((Convert.ToString(paramsArray[i, j], CultureInfo.InvariantCulture)));
                            if (datetimeValue > System.DateTime.MinValue)
                            {
                                stringArray[i, j] = (Convert.ToString(datetimeValue, CultureInfo.InvariantCulture));
                            }
                            else
                            {
                                stringArray[i, j] = (Convert.ToString(paramsArray[i, j], CultureInfo.InvariantCulture));
                            }
                        }
                    }
                }

                if (numOfRows > 0) dbconnect.WriteTable(destinationTableName, stringArray, numOfRows);
            }
            catch (FormatException)
            {
                context.ExecutionInformation.ReportError("Bad format provided in DbWrite step.");
            }

            context.ExecutionInformation.TraceInformation(String.Format("DbWrite inserted data into table {0}", destinationTableName));


            // We are done writing, have the token proceed out of the primary exit
            return ExitType.FirstExit;
        }

        string TryReadState(IState state)
        {
            IRealState realState = state as IRealState;
            if (realState == null)
            {
                IDateTimeState dateTimeState = state as IDateTimeState;
                if (dateTimeState == null)
                {
                    IStringState stringState = state as IStringState;
                    if (stringState == null) return String.Empty;
                    return stringState.Value;
                }
                else
                {
                    return dateTimeState.Value.ToString(CultureInfo.InvariantCulture);
                }
            }
            else
            {
                return realState.Value.ToString();
            }
        }

        DateTime TryAsDateTime(string rawValue)
        {
            DateTime dt = System.DateTime.MinValue;
            if (DateTime.TryParse(rawValue, CultureInfo.InvariantCulture, DateTimeStyles.None, out dt))
            {
                return dt;
            }
            else
            {
                return dt;
            }
        }

        #endregion
    }
}
