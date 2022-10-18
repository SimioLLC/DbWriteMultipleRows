using System;
using SimioAPI;
using SimioAPI.Extensions;

namespace DbReadWriteMultipleRows
{
    class DbExecuteMultipleRowsStepDefinition : IStepDefinition
    {
        #region IStepDefinition Members

        /// <summary>
        /// Property returning the full name for this type of step. The name should contain no spaces. 
        /// </summary>
        public string Name
        {
            get { return "DbExecuteMultipleRows"; }
        }

        /// <summary>
        /// Property returning a short description of what the step does.  
        /// </summary>
        public string Description
        {
            get { return "The DbExecuteMultipleRows step may be used to execute an SQL statement on database."; }
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
        static readonly Guid MY_ID = new Guid("{25228045-230e-4c7f-a938-1c52c7506031}");

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

            // And a format specifier
            pd = schema.AddStringProperty("SQLStatement", String.Empty);
            pd.DisplayName = "SQL Statement";
            pd.Description = "SQL Statement...Use @ sign with an index to specify a parameter in the Item repeating property";
            pd.Required = false;

            // A repeat group of values to write out
            IRepeatGroupPropertyDefinition parts = schema.AddRepeatGroupProperty("Items");
            parts.Description = "The expression items to be written out.";

            pd = parts.PropertyDefinitions.AddExpressionProperty("Expression", String.Empty);
            pd.Description = "Expression value to be written out.";
        }

        /// <summary>
        /// Method called to create a new instance of this step type to place in a process. 
        /// Returns an instance of the class implementing the IStep interface.
        /// </summary>
        public IStep CreateStep(IPropertyReaders properties)
        {
            return new DbExecuteMultipleRowsStep(properties);
        }

        #endregion
    }

    class DbExecuteMultipleRowsStep : IStep
    {
        IPropertyReaders _props;
        IPropertyReader _sqlstatementProp;
        IElementProperty _dbconnectMultipleRowsElementProp;
        IRepeatingPropertyReader _items;
        public DbExecuteMultipleRowsStep(IPropertyReaders properties)
        {
            _props = properties;
            _sqlstatementProp = _props.GetProperty("SQLStatement");
            _dbconnectMultipleRowsElementProp = (IElementProperty)_props.GetProperty("DbConnectMultipleRows");
            _items = (IRepeatingPropertyReader)_props.GetProperty("Items");
        }

        #region IStep Members

        /// <summary>
        /// Method called when a process token executes the step.
        /// </summary>
        public ExitType Execute(IStepExecutionContext context)
        {
            // Get an array of double values from the repeat group's list of expressions
            object[] paramsArray = new object[_items.GetCount(context)];
            for (int i = 0; i < _items.GetCount(context); i++)
            {
                // The thing returned from GetRow is IDisposable, so we use the using() pattern here
                using (IPropertyReaders row = _items.GetRow(i, context))
                {
                    // Get the expression property
                    IExpressionPropertyReader expressionProp = row.GetProperty("Expression") as IExpressionPropertyReader;
                    // Resolve the expression to get the value
                    paramsArray[i] = expressionProp.GetExpressionValue(context);
                }
            }   

            // set DB data
            DBConnectMultipleRowsElement dbconnect = (DBConnectMultipleRowsElement)_dbconnectMultipleRowsElementProp.GetElement(context);
            String sqlString = _sqlstatementProp.GetStringValue(context);

            int numberOfRowsAffected = 0;
            try
            {
                // for each parameter..Backwards to ensure higher parameters are not overwritten by lower parameters
                for (int i = paramsArray.Length - 1; i >= 0; i--)
                {
                    int paramIndex = i + 1;
                    String replaceString = "@" + paramIndex.ToString();
                    sqlString = sqlString.Replace(replaceString, paramsArray[i].ToString());
                }

                numberOfRowsAffected = dbconnect.ExecuteResults(sqlString);
            }
            catch (FormatException)
            {
                context.ExecutionInformation.ReportError("Bad format provided in Db Execute step."); 
            }

            context.ExecutionInformation.TraceInformation(String.Format("DbExecute ran using the SQL statement {0} affecting {1} rows", sqlString, numberOfRowsAffected));

            // We are done writing, have the token proceed out of the primary exit
            return ExitType.FirstExit;
        }

        #endregion
    }
}
