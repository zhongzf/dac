using System;
using System.Collections.Generic;
using System.Linq;
using System.Linq.Expressions;
using System.Reflection;
using System.Text;
using RaisingStudio.Data.Common;
using RaisingStudio.Data.Linq;
using System.Data;

namespace RaisingStudio.Data
{
    public class CommandBuilder : ExpressionVisitor
    {
        public const string CONDITIONCOMMANDAPPENDTEXT = " ";
        /// <summary>
        /// Format string of parameter name.
        /// </summary>
        public const string PARAMETERNAMEFORMAT = "@pe{0}";

        private Expression expression;
        private string tableName;
        private string[] propertyNames;
        private Type[] propertyTypes;
        private string[] columnNames;
        private string[] columnTypes;

        public string[] KeyColumns
        {
            get;
            set;
        }

        private string GetColumnName(string propertyName)
        {
            return this.columnNames[Array.IndexOf<string>(this.propertyNames, propertyName)];
        }

        private string[] GetColumnNames(string[] columns)
        {
            if ((columns != null) && (columns.Length > 0))
            {
                string[] columnNames = new string[columns.Length];
                int index = 0;
                for (int i = 0; i < this.columnNames.Length; i++)
                {
                    if (columns.Contains(this.propertyNames[i]))
                    {
                        columnNames[index++] = this.columnNames[i];
                    }
                }
                return columnNames;
            }
            else
            {
                return this.columnNames;
            }
        }

        public CommandBuilder(Expression expression, string tableName, string[] propertyNames, Type[] propertyTypes, string[] columnNames, string[] columnTypes)
        {
            this.expression = expression;
            this.tableName = tableName;
            this.propertyNames = propertyNames;
            this.propertyTypes = propertyTypes;
            this.columnNames = columnNames;
            this.columnTypes = columnTypes;
        }

        // TODO: add a method to deal with brackets, instead of in anywhere.
        public bool UseBrackets
        {
            get;
            set;
        }

        public string IdentityMethod
        {
            get;
            set;
        }

        public bool SupportsInsertSelectIdentity { get; set; }


        public Command GetIdentityCommand(string column)
        {
            if (!string.IsNullOrEmpty(IdentityMethod))
            {
                switch (IdentityMethod.ToUpper())
                {
                    case "IDENTITY":
                        {
                            return new Command("SELECT @@IDENTITY");
                        }
                    case "SCOPE_IDENTITY":
                        {
                            return new Command("SELECT SCOPE_IDENTITY()");
                        }
                    case "LASTVAL":
                        {
                            return new Command("SELECT LASTVAL()");
                        }
                    case "LAST_INSERT_ROWID":
                        {
                            return new Command("SELECT LAST_INSERT_ROWID()");
                        }
                    case "LAST_INSERT_ID":
                        {
                            return new Command("SELECT LAST_INSERT_ID()");
                        }
                    case "CURRVAL":
                        {
                            return (column != null) ? new Command(string.Format("SELECT {0}_{1}_SEQ.CURRVAL FROM DUAL", this.tableName, GetColumnName(column))) : new Command(string.Format("SELECT {0}_SEQ.CURRVAL FROM DUAL", this.tableName));
                        }
                }
            }
            return null;
        }

        public Command GetSelectCommand(out string[] columns)
        {
            if (this.expression != null)
            {
                if (this.commandSegment == null)
                {
                    BuildCommand(null);
                }
                columns = this.columnList.Count > 0 ? this.columnList.ToArray() : null;
                return this.commandSegment.Command;
            }
            else
            {
                columns = null;
                return null;
            }
        }

        private CommandSegment commandSegment;
        private CommandSegment conditionSegment;

        private Stack<CommandSegment> segmentStack = new Stack<CommandSegment>();
        private int parameterIndex;

        public virtual void BuildCommand(string[] columnNames)
        {
            this.segmentStack.Clear();
            this.parameterIndex = 0;

            if ((expression is MethodCallExpression) && ((expression as MethodCallExpression).Method.Name == "Count") && ((expression as MethodCallExpression).Arguments[0] is ConstantExpression))
            {
                Command command = null;
                this.commandSegment = new CommandSegment(command, CommandSegmentType.Select);
            }
            else
            {
                ExpressionEvaluator expressionEvaluator = new ExpressionEvaluator();
                Expression evaluatedExpression = expressionEvaluator.Evaluate(expression);

                this.Visit(evaluatedExpression);

                CommandSegmentType finalSegmentType = CommandSegmentType.None;
                Command command = null;
                CommandSegment commandSegment = this.segmentStack.Count > 0 ? this.segmentStack.Pop() : null;
                Command condition = null;
                if (commandSegment != null)
                {
                    if (commandSegment.SegmentType == CommandSegmentType.Select)
                    {
                        command = commandSegment.Command;
                        finalSegmentType |= CommandSegmentType.Select;

                        commandSegment = this.segmentStack.Count > 0 ? this.segmentStack.Pop() : null;
                        condition = GenerateConditionCommand(commandSegment);
                    }
                    else if ((commandSegment.SegmentType == CommandSegmentType.Binary) || (commandSegment.SegmentType == CommandSegmentType.Condition))
                    {
                        condition = GenerateConditionCommand(commandSegment);
                    }
                    else
                    {
                        command = GetSelectCommand(this.tableName, columnNames ?? (this.columnList.Count > 0 ? GetColumnNames(this.columnList.ToArray()) : this.columnNames));
                    }
                }
                #region condition
                if ((condition != null) && (!string.IsNullOrEmpty(condition.CommandText)))
                {
                    if (command != null)
                    {
                        command = AppendAdditionCommand(command, condition);
                    }
                    else
                    {
                        command = AppendAdditionCommand(GetSelectCommand(this.tableName, columnNames ?? (this.columnList.Count > 0 ? GetColumnNames(this.columnList.ToArray()) : this.columnNames)), condition);
                    }
                    finalSegmentType |= CommandSegmentType.Condition;
                }
                #endregion
                #region sorting
                Command sorting = GenerateSortingCommand(false);
                if ((sorting != null) && (!string.IsNullOrEmpty(sorting.CommandText)))
                {
                    if (command != null)
                    {
                        command = AppendAdditionCommand(command, sorting);
                    }
                    else
                    {
                        command = AppendAdditionCommand(GetSelectCommand(this.tableName, columnNames ?? (this.columnList.Count > 0 ? GetColumnNames(this.columnList.ToArray()) : this.columnNames)), sorting);
                    }
                    finalSegmentType |= CommandSegmentType.Sorting;
                }
                #endregion
                #region paging
                command = GetPagingCommand(command, this.pagingSkipCount, this.pagingTakeCount);
                finalSegmentType |= CommandSegmentType.Paging;
                if (this.countingPaging)
                {
                    command.CommandText = string.Format("SELECT COUNT(*) FROM ( {0} )", command.CommandText);
                    finalSegmentType |= CommandSegmentType.Counting;
                }
                #endregion
                this.commandSegment = new CommandSegment(command, finalSegmentType);
            }
        }

        public string PagingMethod
        {
            get;
            set;
        }

        public const string PAGING_ROWNUM_TEMP_TABLE_NAME = "ROWNUM_TEMP_TABLE";
        public const string PAGING_ROWNUM_TEMP_COLUMN_NAME = "ROWNUM_TEMP_COLUMN";
        public const string PAGING_TOP_TEMP_TABLE_NAME = "TOP_TEMP_TABLE";
        private int PAGING_TAKE_MAX_COUNT = int.MaxValue;

        protected virtual Command GetPagingCommand(Command command, int pagingSkipCount, int pagingTakeCount)
        {
            if (!string.IsNullOrEmpty(PagingMethod))
            {
                switch (PagingMethod.ToUpper())
                {
                    case "LIMIT":
                        {
                            if (pagingSkipCount >= 0)
                            {
                                if (pagingTakeCount >= 0)
                                {
                                    command.CommandText = string.Format("{0} LIMIT {1}, {2}", command.CommandText, pagingSkipCount, pagingTakeCount);
                                }
                                else
                                {
                                    command.CommandText = string.Format("{0} LIMIT {1}, {2}", command.CommandText, pagingSkipCount, PAGING_TAKE_MAX_COUNT);
                                }
                            }
                            else
                            {
                                if (pagingTakeCount >= 0)
                                {
                                    command.CommandText = string.Format("{0} LIMIT {1}, {2}", command.CommandText, 0, pagingTakeCount);
                                }
                            }
                            break;
                        }
                    case "ROWNUM":
                        {
                            if (pagingSkipCount >= 0)
                            {
                                if (pagingTakeCount >= 0)
                                {
                                    command.CommandText = string.Format("SELECT * FROM ( SELECT {3}.*, ROWNUM {4} FROM ( {0} ) {3} WHERE ROWNUM <= {1}) WHERE {4} > {2}", command.CommandText, pagingTakeCount + pagingSkipCount, pagingSkipCount, PAGING_ROWNUM_TEMP_TABLE_NAME, PAGING_ROWNUM_TEMP_COLUMN_NAME);
                                }
                                else
                                {
                                    command.CommandText = string.Format("SELECT * FROM ( SELECT {3}.*, ROWNUM {4} FROM ( {0} ) {3} ) WHERE {4} > {2}", command.CommandText, PAGING_TAKE_MAX_COUNT, pagingSkipCount, PAGING_ROWNUM_TEMP_TABLE_NAME, PAGING_ROWNUM_TEMP_COLUMN_NAME);
                                }
                            }
                            else
                            {
                                if (pagingTakeCount >= 0)
                                {
                                    command.CommandText = string.Format("SELECT {3}.*, ROWNUM {4} FROM ( {0} ) {3} WHERE ROWNUM <= {1}", command.CommandText, pagingTakeCount, null, PAGING_ROWNUM_TEMP_TABLE_NAME, PAGING_ROWNUM_TEMP_COLUMN_NAME);
                                }
                            }
                            break;
                        }
                    case "TOP":
                        {
                            if (pagingSkipCount > 0)
                            {
                                string commandText = command.CommandText;
                                string columns = commandText.Substring("SELECT ".Length, commandText.IndexOf(" FROM") - "SELECT ".Length);
                                string tableName = this.tableName;
                                string ordering;
                                string sorting;
                                GetSortingColumn(commandText, out ordering, out sorting);

                                if (pagingTakeCount >= 0)
                                {
                                    string pagingCommandText = string.Format("SELECT TOP ({0}) {1} FROM {2} WHERE ({3} > (SELECT MAX({3}) FROM (SELECT TOP ({4}) {3} FROM {2} {5}) {6})) {5}", pagingTakeCount, columns, tableName, sorting, pagingSkipCount, ordering, PAGING_TOP_TEMP_TABLE_NAME);
                                    command.CommandText = pagingCommandText;
                                }
                                else
                                {
                                    string pagingCommandText = string.Format("SELECT {1} FROM {2} WHERE ({3} > (SELECT MAX({3}) FROM (SELECT TOP ({4}) {3} FROM {2} {5}) {6})) {5}", null, columns, tableName, sorting, pagingSkipCount, ordering, PAGING_TOP_TEMP_TABLE_NAME);
                                    command.CommandText = pagingCommandText;
                                }
                            }
                            else
                            {
                                if (pagingTakeCount >= 0)
                                {
                                    if (command.CommandText.StartsWith("SELECT "))
                                    {
                                        var remaining = command.CommandText.Substring("SELECT ".Length);
                                        command.CommandText = string.Format("SELECT TOP ({0}) {1}", pagingTakeCount, remaining);
                                    }
                                    else
                                    {
                                        throw new ArgumentException("Unknown select format");
                                    }
                                }
                            }
                            break;
                        }
                    case "ROW_NUMBER":
                        {
                            if (pagingSkipCount > 0)
                            {
                                string commandText = command.CommandText;
                                string columns = commandText.Substring("SELECT ".Length, commandText.IndexOf(" FROM") - "SELECT ".Length);
                                string tableName = this.tableName;
                                string ordering;
                                string sorting;
                                GetSortingColumn(commandText, out ordering, out sorting);

                                if (pagingTakeCount >= 0)
                                {
                                    string pagingCommandText = string.Format("SELECT {2} FROM (SELECT ROW_NUMBER() OVER({0}) AS {1}, {2} FROM {3}) AS {4} WHERE  {1} > {5} AND {1} <= {6}", ordering, PAGING_ROWNUM_TEMP_COLUMN_NAME, columns, tableName, PAGING_ROWNUM_TEMP_TABLE_NAME, pagingSkipCount, pagingSkipCount + pagingTakeCount);
                                    command.CommandText = pagingCommandText;
                                }
                                else
                                {
                                    string pagingCommandText = string.Format("SELECT {2} FROM (SELECT ROW_NUMBER() OVER({0}) AS {1}, {2} FROM {3}) AS {4} WHERE  {1} > {5}", ordering, PAGING_ROWNUM_TEMP_COLUMN_NAME, columns, tableName, PAGING_ROWNUM_TEMP_TABLE_NAME, pagingSkipCount);
                                    command.CommandText = pagingCommandText;
                                }
                            }
                            else
                            {
                                if (pagingTakeCount >= 0)
                                {
                                    if (command.CommandText.StartsWith("SELECT "))
                                    {
                                        var remaining = command.CommandText.Substring("SELECT ".Length);
                                        command.CommandText = string.Format("SELECT TOP ({0}) {1}", pagingTakeCount, remaining);
                                    }
                                    else
                                    {
                                        throw new ArgumentException("Unknown select format");
                                    }
                                }
                            }
                            break;
                        }
                }
            }

            return command;
        }

        private static void GetSortingColumn(string commandText, out string ordering, out string sorting)
        {
            ordering = null;
            sorting = null;
            int index = commandText.IndexOf("ORDER BY ");
            if (index != -1)
            {
                ordering = commandText.Substring(index);
                sorting = ordering.Substring("ORDER BY ".Length);
                if (sorting.EndsWith(" ASC", StringComparison.InvariantCultureIgnoreCase))
                {
                    sorting = sorting.Substring(0, sorting.Length - " ASC".Length);
                }
                else if (sorting.EndsWith(" DESC", StringComparison.InvariantCultureIgnoreCase))
                {
                    sorting = sorting.Substring(0, sorting.Length - " DESC".Length);
                }
            }
        }

        public virtual void BuildCondition(int parameterIndex)
        {
            this.segmentStack.Clear();
            this.parameterIndex = parameterIndex;


            ExpressionEvaluator expressionEvaluator = new ExpressionEvaluator();
            Expression evaluatedExpression = expressionEvaluator.Evaluate(expression);

            this.Visit(evaluatedExpression);

            CommandSegmentType finalSegmentType = CommandSegmentType.None;
            Command command = null;
            CommandSegment commandSegment = this.segmentStack.Count > 0 ? this.segmentStack.Pop() : null;
            Command condition = null;
            if (commandSegment != null)
            {
                if (commandSegment.SegmentType == CommandSegmentType.Select)
                {
                    command = commandSegment.Command;
                    finalSegmentType |= CommandSegmentType.Select;

                    commandSegment = this.segmentStack.Count > 0 ? this.segmentStack.Pop() : null;
                    condition = GenerateConditionCommand(commandSegment);
                }
                else if ((commandSegment.SegmentType == CommandSegmentType.Binary) || (commandSegment.SegmentType == CommandSegmentType.Condition))
                {
                    condition = GenerateConditionCommand(commandSegment);
                }
            }
            #region condition
            if ((condition != null) && (!string.IsNullOrEmpty(condition.CommandText)))
            {
                command = condition;
                finalSegmentType |= CommandSegmentType.Condition;
            }
            #endregion
            this.conditionSegment = new CommandSegment(command, finalSegmentType);
        }


        protected virtual Command GetSelectCommand(string tableName, string[] columnNames)
        {
            var selectCommandTextStringBuilder = new StringBuilder();
            selectCommandTextStringBuilder.Append("SELECT ");
            if (columnNames == null)
            {
                selectCommandTextStringBuilder.Append("*");
            }
            else
            {
                for (int i = 0; i < columnNames.Length; i++)
                {
                    if (i > 0)
                    {
                        selectCommandTextStringBuilder.Append(", ");
                    }
                    selectCommandTextStringBuilder.AppendFormat(UseBrackets ? "[{0}]" : "{0}", columnNames[i]);
                }
            }
            selectCommandTextStringBuilder.AppendFormat(UseBrackets ? " FROM [{0}]" : " FROM {0}", tableName);
            return new Command(selectCommandTextStringBuilder.ToString());
        }

        protected virtual Command GetCountCommand(string tableName)
        {
            return new Command(string.Format(UseBrackets ? "SELECT COUNT(*) FROM [{0}]" : "SELECT COUNT(*) FROM {0}", tableName));
        }

        protected virtual Command GetFunctionCommand(string tableName, string function, string columnName)
        {
            return new Command(string.Format(UseBrackets ? "SELECT {0}([{1}]) FROM [{2}]" : "SELECT {0}({1}) FROM {2}", function, columnName, tableName));
        }


        public virtual Command AppendAdditionCommand(Command command, Command addtion)
        {
            if (addtion != null)
            {
                command.CommandText = string.Format("{0}{1}{2}", command.CommandText, CONDITIONCOMMANDAPPENDTEXT, addtion.CommandText);
                foreach (Parameter parameter in addtion.Parameters)
                {
                    command.Parameters.Add(parameter);
                }
            }
            return command;
        }

        protected virtual Command GenerateConditionCommand(CommandSegment commandSegment)
        {
            Command condition = null;
            if (commandSegment != null)
            {
                if (commandSegment.SegmentType == CommandSegmentType.Condition)
                {
                    condition = commandSegment.Command;
                }
                else if (commandSegment.SegmentType == CommandSegmentType.Binary)
                {
                    string conditionCommandText = string.Format("WHERE {0}", commandSegment.Command.CommandText);
                    condition = new Command(conditionCommandText);
                    foreach (Parameter parameter in commandSegment.Command.Parameters)
                    {
                        condition.Parameters.Add(parameter);
                    }
                }
            }
            return condition;
        }

        protected virtual Command GenerateSortingCommand(bool sourceName)
        {
            Command sortingCommand = new Command();
            StringBuilder sortingCommandTextStringBuilder = new StringBuilder();
            if (this.sortingList.Count > 0)
            {
                sortingCommandTextStringBuilder.Append("ORDER BY ");

                bool hasSorting = false;
                foreach (string[] sorting in this.sortingList)
                {
                    string sortingExpressionString = string.Format(UseBrackets ? "[{0}] {1}" : "{0} {1}", sourceName ? sorting[0] : GetColumnName(sorting[0]), sorting[1]);
                    if (!hasSorting)
                    {
                        hasSorting = true;
                    }
                    else
                    {
                        sortingCommandTextStringBuilder.Append(", ");
                    }
                    sortingCommandTextStringBuilder.Append(sortingExpressionString);
                }
            }
            sortingCommand.CommandText = sortingCommandTextStringBuilder.ToString();
            return sortingCommand;
        }
        

        protected override Expression VisitBinary(BinaryExpression b)
        {
            if (b != null)
            {
                #region for IS NULL, IS NOT NULL
                if (((b.NodeType == ExpressionType.Equal) || (b.NodeType == ExpressionType.NotEqual)) && ((b.Right.NodeType == ExpressionType.Constant) && (((ConstantExpression)b.Right).Value == null)))
                {
                    this.Visit(b.Left);
                    Command left = this.segmentStack.Pop().Command;
                    string commandText = String.Format("({0} {1})", left, b.NodeType == ExpressionType.Equal ? "IS NULL" : "IS NOT NULL");
                    Command command = new Command(commandText);
                    foreach (var parameter in left.Parameters)
                    {
                        command.Parameters.Add(parameter);
                    }
                    this.segmentStack.Push(new CommandSegment(command, CommandSegmentType.Binary));
                }
                #endregion
                #region
                else
                {
                    string operatorText;
                    switch (b.NodeType)
                    {
                        case ExpressionType.Equal:
                            {
                                operatorText = "=";
                                break;
                            }
                        case ExpressionType.NotEqual:
                            {
                                operatorText = "<>";
                                break;
                            }
                        case ExpressionType.GreaterThan:
                            {
                                operatorText = ">";
                                break;
                            }
                        case ExpressionType.GreaterThanOrEqual:
                            {
                                operatorText = ">=";
                                break;
                            }
                        case ExpressionType.LessThan:
                            {
                                operatorText = "<";
                                break;
                            }
                        case ExpressionType.LessThanOrEqual:
                            {
                                operatorText = "<=";
                                break;
                            }

                        case ExpressionType.And:
                        case ExpressionType.AndAlso:
                            {
                                operatorText = "AND";
                                break;
                            }
                        case ExpressionType.Or:
                        case ExpressionType.OrElse:
                            {
                                operatorText = "OR";
                                break;
                            }

                        case ExpressionType.Add:
                            {
                                operatorText = "+";
                                break;
                            }
                        case ExpressionType.Subtract:
                            {
                                operatorText = "-";
                                break;
                            }
                        case ExpressionType.Multiply:
                            {
                                operatorText = "*";
                                break;
                            }
                        case ExpressionType.Divide:
                            {
                                operatorText = "/";
                                break;
                            }
                        case ExpressionType.Modulo:
                            {
                                operatorText = "%";
                                break;
                            }

                        default:
                            {
                                throw new NotSupportedException(b.NodeType + " is not supported.");
                            }
                    }

                    this.Visit(b.Left);
                    this.Visit(b.Right);

                    Command right = this.segmentStack.Pop().Command;
                    Command left = this.segmentStack.Pop().Command;

                    string commandText = String.Format("({0} {1} {2})", left, operatorText, right);
                    Command command = new Command(commandText);
                    foreach (var parameter in left.Parameters)
                    {
                        command.Parameters.Add(parameter);
                    }
                    foreach (var parameter in right.Parameters)
                    {
                        command.Parameters.Add(parameter);
                    }

                    this.segmentStack.Push(new CommandSegment(command, CommandSegmentType.Binary));
                }
                #endregion
            }
            return b;
        }

        protected override Expression VisitConstant(ConstantExpression c)
        {
            if (c != null)
            {
                string parameterName = string.Format(PARAMETERNAMEFORMAT, this.parameterIndex++);
                DbType dbType = DbType.String;
                object parameterValue = c.Value;
                Command command = new Command(parameterName);
                if ((columnStack.Count > 0) && (this.segmentStack.Count > 0))
                {
                    string column = this.columnStack.Pop();
                    string columnType = this.columnTypes[Array.IndexOf<string>(this.propertyNames, column)];
                    if (parameterValue != null)
                    {
                        if (!string.IsNullOrWhiteSpace(columnType))
                        {
                            dbType = TypeManager.GetWellKnownDbType(columnType);
                            var converter = ConverterManager.Default.GetConverter(parameterValue.GetType(), dbType);
                            if (converter != null)
                            {
                                parameterValue = converter(parameterValue);
                            }
                        }
                    }
                }
                command.Parameters.Add(parameterName, dbType, parameterValue);
                this.segmentStack.Push(new CommandSegment(command, CommandSegmentType.Parameter));
            }
            return c;
        }

        protected Stack<string> columnStack = new Stack<string>();

        protected override Expression VisitMember(MemberExpression m)
        {
            if (m != null)
            {
                PropertyInfo propertyInfo = m.Member as PropertyInfo;
                if (propertyInfo != null)
                {
                    this.segmentStack.Push(new CommandSegment(new Command(string.Format(UseBrackets ? "[{0}]" : "{0}", GetColumnName(propertyInfo.Name))), CommandSegmentType.Column));
                    this.columnStack.Push(propertyInfo.Name);
                }
            }
            return m;
        }

        protected List<string[]> sortingList = new List<string[]>();
        protected List<string> columnList = new List<string>();

        private int pagingSkipCount = -1;
        private int pagingTakeCount = -1;
        private bool countingPaging = false;

        protected override Expression VisitMethodCall(MethodCallExpression m)
        {
            if (m != null)
            {
                MethodInfo methoInfo = m.Method;
                if (methoInfo != null)
                {
                    switch (methoInfo.Name)
                    {
                        #region order
                        case "OrderBy":
                        case "ThenBy":
                            {
                                ProcessOrderingExpression(m, "ASC");
                                return m;
                            }
                        case "OrderByDescending":
                        case "ThenByDescending":
                            {
                                ProcessOrderingExpression(m, "DESC");
                                return m;
                            }
                        #endregion
                        #region select
                        case "Select":
                            {
                                if ((m.Arguments != null) && (m.Arguments.Count > 1))
                                {
                                    this.Visit(m.Arguments[0]);
                                    string[] columns = GetColumns(m.Arguments[1]);
                                    if (columns != null)
                                    {
                                        this.columnList.AddRange(columns);
                                    }
                                }
                                return m;
                            }
                        #endregion
                        #region count
                        case "Count":
                            {
                                this.Visit(m.Arguments[0]);
                                if (this.pagingSkipCount >= 0 || this.pagingTakeCount >= 0)
                                {
                                    this.countingPaging = true;
                                }
                                else
                                {
                                    Command command = this.GetCountCommand(this.tableName);
                                    commandSegment = this.segmentStack.Count > 0 ? this.segmentStack.Pop() : null;
                                    Command condition = GenerateConditionCommand(commandSegment);
                                    if ((condition != null) && (!string.IsNullOrEmpty(condition.CommandText)))
                                    {
                                        command = this.AppendAdditionCommand(command, condition);
                                    }
                                    this.segmentStack.Push(new CommandSegment(command, CommandSegmentType.Select));
                                }
                                return m;
                            }
                        #endregion
                        #region functions
                        case "Average":
                            {
                                ProcessFunctionExpression(m, "AVG");
                                return m;
                            }
                        case "Max":
                            {
                                ProcessFunctionExpression(m, "MAX");
                                return m;
                            }
                        case "Min":
                            {
                                ProcessFunctionExpression(m, "MIN");
                                return m;
                            }
                        case "Sum":
                            {
                                ProcessFunctionExpression(m, "SUM");
                                return m;
                            }
                        #endregion
                        #region string
                        case "Contains":
                            {
                                if (m.Method.DeclaringType == typeof(string))
                                {
                                    ProcessStringExpression(m, "%{0}%");
                                }
                                else
                                {
                                    ProcessContainsExpression(m);
                                }
                                return m;
                            }
                        case "StartWith":
                            {
                                ProcessStringExpression(m, "{0}%");
                                return m;
                            }
                        case "EndWith":
                            {
                                ProcessStringExpression(m, "%{0}");
                                return m;
                            }
                        #endregion
                        #region paging
                        case "Skip":
                            {
                                ProcessPagingExpression(m, ref this.pagingSkipCount);
                                return m;
                            }
                        case "Take":
                            {
                                ProcessPagingExpression(m, ref this.pagingTakeCount);
                                return m;
                            }
                        #endregion
                        case "Not":
                            {
                                return m;
                            }
                    }
                }
            }
            return base.VisitMethodCall(m);
        }

        protected virtual void ProcessOrderingExpression(MethodCallExpression m, string ordering)
        {
            this.Visit(m.Arguments[0]);
            if ((m.Arguments != null) && (m.Arguments.Count > 1))
            {
                this.Visit(m.Arguments[1]);
                if (this.columnStack.Count > 0)
                {
                    string column = this.columnStack.Pop();
                    this.sortingList.Add(new[] { column, ordering });
                    this.segmentStack.Pop();
                }
                else if (this.segmentStack.Count > 0)
                {
                    string column = (string)this.segmentStack.Pop().Command.Parameters[0].Value;
                    this.sortingList.Add(new[] { column, ordering });
                }
            }
        }

        protected virtual void ProcessFunctionExpression(MethodCallExpression m, string function)
        {
            this.Visit(m.Arguments[0]);
            if ((this.columnList != null) && (this.columnList.Count > 0))
            {
                string column = (string)this.columnList[0];
                Command command = this.GetFunctionCommand(this.tableName, function, GetColumnName(column));
                commandSegment = this.segmentStack.Count > 0 ? this.segmentStack.Pop() : null;
                Command condition = GenerateConditionCommand(commandSegment);
                if ((condition != null) && (!string.IsNullOrEmpty(condition.CommandText)))
                {
                    command = this.AppendAdditionCommand(command, condition);
                }
                this.segmentStack.Push(new CommandSegment(command, CommandSegmentType.Select));
            }
        }

        protected virtual void ProcessStringExpression(MethodCallExpression m, string format)
        {
            this.Visit(m.Object);
            this.Visit(m.Arguments[0]);

            Command right = this.segmentStack.Pop().Command;
            Command left = this.segmentStack.Pop().Command;
            Command command = new Command(string.Format("{0} LIKE {1}", left.CommandText, right.CommandText));
            foreach (var parameter in left.Parameters)
            {
                command.Parameters.Add(parameter);
            }
            foreach (var parameter in right.Parameters)
            {
                parameter.Value = string.Format(format, parameter.Value);
                command.Parameters.Add(parameter);
            }
            this.segmentStack.Push(new CommandSegment(command, CommandSegmentType.Binary));
        }

        protected virtual void ProcessPagingExpression(MethodCallExpression m, ref int count)
        {
            this.Visit(m.Arguments[0]);
            this.Visit(m.Arguments[1]);

            Command right = this.segmentStack.Pop().Command;
            if (right.HasParameters)
            {
                count = Convert.ToInt32(right.Parameters[0].Value);
            }
            if (((this.PagingMethod == "TOP") || (this.PagingMethod == "ROW_NUMBER")) && (this.sortingList.Count == 0))
            {
                if ((KeyColumns != null) && (KeyColumns.Length == 1))
                {
                    this.sortingList.Add(new[] { KeyColumns[0], "ASC" });
                }
                else
                {
                    if (this.columnNames.Length > 0)
                    {
                        this.sortingList.Add(new[] { this.columnNames[0], "ASC" });
                    }
                    else
                    {
                        throw new ArgumentException("Please setup one sorting column.");
                    }
                }
            }
        }

        protected virtual void ProcessContainsExpression(MethodCallExpression m)
        {
            this.Visit(m.Arguments[0]);
            this.Visit(m.Arguments[1]);

            Command right = this.segmentStack.Pop().Command;
            Command left = this.segmentStack.Pop().Command;
            object[] values = left.Parameters[0].Value as object[];
            if ((values != null) && (values.Length > 0))
            {
                string[] parameterNames = new string[values.Length];
                for (int i = 0; i < values.Length; i++)
                {
                    string parameterName = string.Format(PARAMETERNAMEFORMAT, this.parameterIndex++);
                    parameterNames[i] = parameterName;
                }
                Command command = new Command(string.Format("{0} IN ({1})", right.CommandText, string.Join(",", parameterNames)));
                foreach (var parameter in right.Parameters)
                {
                    command.Parameters.Add(parameter);
                }
                for (int i = 0; i < values.Length; i++)
                {
                    command.Parameters.Add(parameterNames[i], values[i]);
                }
                this.segmentStack.Push(new CommandSegment(command, CommandSegmentType.Binary));
            }
            else
            {
                throw new ArgumentNullException("values", "There is no values for Contains expression.");
            }
        }


        protected override Expression VisitUnary(UnaryExpression node)
        {
            switch (node.NodeType)
            {
                case ExpressionType.Not:
                    {
                        this.Visit(node.Operand);
                        Command operand = this.segmentStack.Pop().Command;
                        if (operand.CommandText.Contains(" IN "))
                        {
                            Command command = new Command(operand.CommandText.Replace(" IN ", " NOT IN "));
                            foreach (var parameter in operand.Parameters)
                            {
                                command.Parameters.Add(parameter);
                            }
                            this.segmentStack.Push(new CommandSegment(command, CommandSegmentType.Binary));
                        }
                        else
                        {
                            Command command = new Command(string.Format("(NOT ({0}))", operand));
                            foreach (var parameter in operand.Parameters)
                            {
                                command.Parameters.Add(parameter);
                            }
                            this.segmentStack.Push(new CommandSegment(command, CommandSegmentType.Binary));
                        }
                        return node;
                    }
                default:
                    {
                        return base.VisitUnary(node);
                    }
            }            
        }


        public static string[] GetColumns(Expression expression)
        {
            if ((expression is UnaryExpression) && (expression.NodeType == ExpressionType.Quote))
            {
                expression = (expression as UnaryExpression).Operand;
            }
            if ((expression is LambdaExpression) && (expression.NodeType == ExpressionType.Lambda))
            {
                LambdaExpression l = expression as LambdaExpression;
                Expression e = l.Body;
                if (e.NodeType == ExpressionType.Convert)
                {
                    e = (e as UnaryExpression).Operand;
                }
                if ((e is MemberExpression) && (e.NodeType == ExpressionType.MemberAccess))
                {
                    MemberExpression m = e as MemberExpression;
                    return new string[] { m.Member.Name };
                }
                else if ((e is NewExpression) && (e.NodeType == ExpressionType.New))
                {
                    NewExpression n = e as NewExpression;
                    List<string> columnList = new List<string>();
                    foreach (var a in n.Arguments)
                    {
                        if ((a is MemberExpression) && (a.NodeType == ExpressionType.MemberAccess))
                        {
                            MemberExpression m = a as MemberExpression;
                            columnList.Add(m.Member.Name);
                        }
                    }
                    return columnList.ToArray();
                }
                else if ((e is NewArrayExpression) && (e.NodeType == ExpressionType.NewArrayInit))
                {
                    NewArrayExpression n = e as NewArrayExpression;
                    List<string> columnList = new List<string>();
                    foreach (var a in n.Expressions)
                    {
                        if ((a is MemberExpression) && (a.NodeType == ExpressionType.MemberAccess))
                        {
                            MemberExpression m = a as MemberExpression;
                            columnList.Add(m.Member.Name);
                        }
                    }
                    return columnList.ToArray();
                }
            }
            return null;
        }

        
        public virtual Command GetInsertCommand(string[] columns)
        {
            string[] columnNames = GetColumnNames(columns);
            var insertCommandTextStringBuilder = new StringBuilder();
            var columnStringBuilder = new StringBuilder();
            var parameterStringBuilder = new StringBuilder();
            var command = new Command();
            for (int i = 0; i < columnNames.Length; i++)
            {
                string columnName = columnNames[i];
                if (i > 0)
                {
                    columnStringBuilder.Append(", ");
                    parameterStringBuilder.Append(", ");
                }
                columnStringBuilder.AppendFormat(UseBrackets ? "[{0}]" : "{0}", columnName);

                string parameterName = string.Format(PARAMETERNAMEFORMAT, i + 1);
                parameterStringBuilder.Append(parameterName);

                command.AddParameter(parameterName, TypeManager.GetWellKnownDbType(columnTypes[Array.IndexOf(this.columnNames, columnName)]));
            }
            insertCommandTextStringBuilder.AppendFormat(UseBrackets ? "INSERT INTO [{0}] ({1}) VALUES ({2})" : "INSERT INTO {0} ({1}) VALUES ({2})", tableName, columnStringBuilder, parameterStringBuilder);
            command.CommandText = insertCommandTextStringBuilder.ToString();
            return command;
        }

        public virtual Command GetDeleteCommand()
        {
            if (this.expression != null)
            {
                if (this.commandSegment == null)
                {
                    BuildCondition(0);
                }
                return AppendAdditionCommand(new Command(string.Format(UseBrackets ? "DELETE FROM [{0}] " : "DELETE FROM {0} ", tableName)), this.conditionSegment.Command);
            }
            else
            {
                return null;
            }
        }

        private static void AppendCondition(Command command, string[] keyColumnNames, string[] keyColumnTypes, object[] primaryKeys, int parameterIndex)
        {
            StringBuilder commandTextStringBuilder = new StringBuilder();
            commandTextStringBuilder.Append(" WHERE ");
            for (int i = 0; i < keyColumnNames.Length; i++)
            {
                string parameterName = string.Format(PARAMETERNAMEFORMAT, i + parameterIndex);
                if (i > 0)
                {
                    commandTextStringBuilder.Append(" AND ");
                }
                commandTextStringBuilder.AppendFormat("{0} = {1}", keyColumnNames[i], parameterName);

                command.AddParameter(parameterName, TypeManager.GetWellKnownDbType(keyColumnTypes[i]), primaryKeys[i]);
            }
            command.CommandText += commandTextStringBuilder.ToString();
        }

        public virtual Command GetDeleteCommand(string[] keyColumnNames, string[] keyColumnTypes, object[] primaryKeys)
        {
            var deleteCommand = new Command(string.Format(UseBrackets ? "DELETE FROM [{0}]" : "DELETE FROM {0}", tableName));
            AppendCondition(deleteCommand, keyColumnNames, keyColumnTypes, primaryKeys, 0);
            return deleteCommand;
        }

        public virtual Command GetUpdateCommmand(string[] columnNames, string[] keyColumnNames, bool updateKeys, out int parameterIndex)
        {
            StringBuilder updateCommandTextStringBuilder = new StringBuilder();
            updateCommandTextStringBuilder.AppendFormat(UseBrackets ? "UPDATE [{0}] SET " : "UPDATE {0} SET ", tableName);
            Command command = new Command();
            parameterIndex = 1;
            for (int i = 0; i < columnNames.Length; i++)
            {
                string columName = columnNames[i];
                if (updateKeys || (!keyColumnNames.Contains(columName)))
                {
                    if (parameterIndex > 1)
                    {
                        updateCommandTextStringBuilder.Append(", ");
                    }
                    string parameterName = string.Format(PARAMETERNAMEFORMAT, parameterIndex++);
                    updateCommandTextStringBuilder.AppendFormat(UseBrackets ? "[{0}] = {1}" : "{0} = {1}", columName, parameterName);

                    command.AddParameter(parameterName, TypeManager.GetWellKnownDbType(columnTypes[Array.IndexOf(this.columnNames, columName)]));
                }
            }
            command.CommandText = updateCommandTextStringBuilder.ToString();
            return command;
        }

        public virtual Command GetUpdateCommand(string[] keyColumnNames, string[] keyColumnTypes, object[] primaryKeys, bool updateKeys)
        {
            int parameterIndex;
            Command updateCommand = GetUpdateCommmand(this.columnNames, keyColumnNames, updateKeys, out parameterIndex);            
            AppendCondition(updateCommand, keyColumnNames, keyColumnTypes, primaryKeys, parameterIndex);
            return updateCommand;
        }

        public virtual Command GetUpdateCommand(string[] columns)
        {
            string[] columnNames = GetColumnNames(columns); 
            int parameterIndex;
            Command updateCommand = GetUpdateCommmand(columnNames, null, true, out parameterIndex);            

            if (this.expression != null)
            {
                if (this.commandSegment == null)
                {
                    BuildCondition(parameterIndex);
                }
                return AppendAdditionCommand(updateCommand, this.conditionSegment.Command);
            }
            else
            {
                return null;
            }
        }

        public virtual Command GetSelectCommand(string[] keyColumnNames, string[] keyColumnTypes, object[] primaryKeys)
        {
            var selectCommandTextStringBuilder = new StringBuilder();
            selectCommandTextStringBuilder.Append("SELECT ");
            var selectCommand = new Command();
            for (int i = 0; i < columnNames.Length; i++)
            {
                if (i > 0)
                {                    
                    selectCommandTextStringBuilder.Append(", ");
                }
                selectCommandTextStringBuilder.AppendFormat(UseBrackets ? "[{0}]" : "{0}", columnNames[i]);
            }
            selectCommandTextStringBuilder.AppendFormat(UseBrackets ? " FROM [{0}]" : " FROM {0}", tableName);
            selectCommand.CommandText = selectCommandTextStringBuilder.ToString();
            AppendCondition(selectCommand, keyColumnNames, keyColumnTypes, primaryKeys, 0);
            return selectCommand;
        }

        public virtual Command GetSelectCommand(string[] columns, out string[] columnNames)
        {
            columnNames = GetColumnNames(columns); 
            if (this.expression != null)
            {
                if (this.commandSegment == null)
                {
                    BuildCommand(columnNames);
                }
                return this.commandSegment.Command;
            }
            else
            {
                return GetSelectCommand(this.tableName, columnNames);
            }
        }

        public virtual Command GetUpdateCommand(string[] columns, string[] keyColumnNames, string[] keyColumnTypes, object[] primaryKeys, bool updateKeys)
        {
            string[] columnNames = GetColumnNames(columns);
            int parameterIndex;
            Command updateCommand = GetUpdateCommmand(columnNames, keyColumnNames, updateKeys, out parameterIndex);
            AppendCondition(updateCommand, keyColumnNames, keyColumnTypes, primaryKeys, parameterIndex);
            return updateCommand;
        }

        public virtual Command GetSelectCountCommand()
        {
            if (this.expression != null)
            {
                if (this.commandSegment == null)
                {
                    BuildCondition(0);
                }
                return AppendAdditionCommand(new Command(string.Format(UseBrackets ? "SELECT COUNT(*) FROM [{0}] " : "SELECT COUNT(*) FROM {0} ", tableName)), this.conditionSegment.Command);
            }
            else
            {
                return new Command(string.Format(UseBrackets ? "SELECT COUNT(*) FROM [{0}]" : "SELECT COUNT(*) FROM {0}", tableName));
            }
        }

        public virtual Command GetSelectFunctionCommand(string function, string column)
        {
            Command command = new Command(string.Format(UseBrackets ? "SELECT {0}([{1}]) FROM [{2}]" : "SELECT {0}({1}) FROM {2}", function, GetColumnName(column), tableName));
            if (this.expression != null)
            {
                command.CommandText += " ";
                if (this.commandSegment == null)
                {
                    BuildCondition(0);
                }
                return AppendAdditionCommand(command, this.conditionSegment.Command);
            }
            return command;
        }

        public virtual Command GetSelectCountCommand(string[] keyColumnNames, string[] keyColumnTypes, object[] primaryKeys)
        {
            var selectCommand = new Command(string.Format(UseBrackets ? "SELECT COUNT(*) FROM [{0}]" : "SELECT COUNT(*) FROM {0}", tableName));
            AppendCondition(selectCommand, keyColumnNames, keyColumnTypes, primaryKeys, 0);
            return selectCommand;
        }


        public Command GetMappingCommand<T>(Command command)
        {
            if (command != null)
            {
                Command mappingCommand =new Command();
                if (command.CommandText != null)
                {
                    mappingCommand.CommandText = command.CommandText.Replace(string.Format(command.UseBrackets ? "[{0}]" : "{0}", typeof(T).Name), string.Format(UseBrackets ? "[{0}]" : "{0}", this.tableName));
                    for (int i = 0; i < this.propertyNames.Length; i++)
                    {
                        string propertyName = this.propertyNames[i];
                        string columnName = this.columnNames[i];
                        mappingCommand.CommandText = mappingCommand.CommandText.Replace(string.Format(command.UseBrackets ? "[{0}]" : "{0}", propertyName), string.Format(UseBrackets ? "[{0}]" : "{0}", columnName));
                    }
                    foreach (Parameter parameter in command.Parameters)
                    {
                        mappingCommand.Parameters.Add(parameter);
                    }
                }
                return mappingCommand;
            }
            return null;
        }


        protected virtual Command GetConditionCommand(string[] searchColumns, string[] searchOperators, object[] searchValues, string[] logicalOperators)
        {
            Command condition = new Command();
            int parameterIndex = 1;
            string logicalOperator = "AND";
            StringBuilder commandTextStringBuilder = new StringBuilder();
            if ((searchColumns != null) && (searchColumns.Length > 0))
            {
                for (int i = 0; i < searchColumns.Length; i++)
                {
                    string columName = searchColumns[i];
                    if ((logicalOperators != null) && (logicalOperators.Length > i))
                    {
                        logicalOperator = logicalOperators[i];
                    }
                    if (i > 0)
                    {
                        commandTextStringBuilder.AppendFormat(" {0} ", logicalOperator);
                    }
                    string operatorText = null;
                    string searchOperator = searchOperators[i];
                    #region
                    switch (searchOperator)
                    {
                        case "eq":
                            {
                                operatorText = "=";
                                break;
                            }
                        case "ne":
                            {
                                operatorText = "<>";
                                break;
                            }
                        case "lt":
                            {
                                operatorText = "<";
                                break;
                            }
                        case "le":
                            {
                                operatorText = "<=";
                                break;
                            }
                        case "gt":
                            {
                                operatorText = ">";
                                break;
                            }
                        case "ge":
                            {
                                operatorText = ">=";
                                break;
                            }
                    }
                    #endregion
                    if (operatorText != null)
                    {
                        string parameterName = string.Format(PARAMETERNAMEFORMAT, parameterIndex++);
                        commandTextStringBuilder.AppendFormat(UseBrackets ? "[{0}] {1} {2}" : "{0} {1} {2}", columName, operatorText, parameterName);
                        condition.AddParameter(parameterName, searchValues[i]);
                    }
                    else
                    {
                        string searchValue = null;
                        #region
                        switch (searchOperator)
                        {
                            case "bw":
                                {
                                    operatorText = "LIKE";
                                    searchValue = string.Format("{0}%", searchValues[i]);
                                    break;
                                }
                            case "bn":
                                {
                                    operatorText = "NOT LIKE";
                                    searchValue = string.Format("{0}%", searchValues[i]);
                                    break;
                                }
                            case "ew":
                                {
                                    operatorText = "LIKE";
                                    searchValue = string.Format("%{0}", searchValues[i]);
                                    break;
                                }
                            case "en":
                                {
                                    operatorText = "NOT LIKE";
                                    searchValue = string.Format("%{0}", searchValues[i]);
                                    break;
                                }
                            case "cn":
                                {
                                    operatorText = "LIKE";
                                    searchValue = string.Format("%{0}%", searchValues[i]);
                                    break;
                                }
                            case "nc":
                                {
                                    operatorText = "NOT LIKE";
                                    searchValue = string.Format("%{0}%", searchValues[i]);
                                    break;
                                }
                        }
                        #endregion
                        if (operatorText != null)
                        {
                            string parameterName = string.Format(PARAMETERNAMEFORMAT, parameterIndex++);
                            commandTextStringBuilder.AppendFormat(UseBrackets ? "[{0}] {1} {2}" : "{0} {1} {2}", columName, operatorText, parameterName);
                            condition.AddParameter(parameterName, searchValue);
                        }
                        else
                        {
                            #region
                            switch (searchOperator)
                            {
                                case "nu":
                                    {
                                        operatorText = "IS NULL";
                                        break;
                                    }
                                case "nn":
                                    {
                                        operatorText = "IS NOT NULL";
                                        break;
                                    }
                            }
                            #endregion
                            if (operatorText != null)
                            {
                                commandTextStringBuilder.AppendFormat(UseBrackets ? "[{0}] {1}" : "{0} {1}", columName, operatorText);
                            }
                            else
                            {
                                #region
                                switch (searchOperator)
                                {
                                    case "in":
                                        {
                                            operatorText = "IN";
                                            break;
                                        }
                                    case "ni":
                                        {
                                            operatorText = "NOT IN";
                                            break;
                                        }
                                }
                                #endregion
                                if (operatorText != null)
                                {
                                    string parameterName = string.Format(PARAMETERNAMEFORMAT, parameterIndex++);
                                    commandTextStringBuilder.AppendFormat(UseBrackets ? "[{0}] {1} {2}" : "{0} {1} {2}", columName, operatorText, parameterName);
                                    condition.AddParameter(parameterName, searchValues[i]);
                                }
                                else
                                {
                                    throw new ArgumentException("search operator [{0}] is unkown.", searchOperator);
                                }
                            }
                        }
                    }
                }
            }
            condition.CommandText = commandTextStringBuilder.ToString();
            if (!string.IsNullOrWhiteSpace(condition.CommandText))
            {
                condition.CommandText = string.Format("WHERE {0}", condition.CommandText);
            }
            return condition;
        }


        public Command GetQueryCommand(string[] searchColumns, string[] searchOperators, object[] searchValues, string[] logicalOperators, string[] sortColumns, string[] sortOrdering, int pagingSkipCount, int pagingTakeCount)
        {
            Command command = GetSelectCommand(this.tableName, this.columnNames);
            #region condition
            Command condition = GetConditionCommand(searchColumns, searchOperators, searchValues, logicalOperators);
            if ((condition != null) && (!string.IsNullOrEmpty(condition.CommandText)))
            {
                command = AppendAdditionCommand(command, condition);
            }
            #endregion
            #region sorting
            this.sortingList.Clear();
            if (sortColumns != null)
            {
                for (int i = 0; i < sortColumns.Length; i++)
                {
                    string sortColumn = sortColumns[i];
                    if (!string.IsNullOrWhiteSpace(sortColumn))
                    {
                        string ordering = sortOrdering.Length > i ? sortOrdering[i] : "ASC";
                        if (!string.Equals(ordering, "ASC", StringComparison.OrdinalIgnoreCase) && !string.Equals(ordering, "DESC", StringComparison.OrdinalIgnoreCase))
                        {
                            ordering = "ASC";
                        }
                        this.sortingList.Add(new string[] { sortColumn, ordering });
                    }
                }
            }
            Command sorting = GenerateSortingCommand(true);
            if ((sorting != null) && (!string.IsNullOrEmpty(sorting.CommandText)))
            {
                command = AppendAdditionCommand(command, sorting);
            }
            #endregion
            #region paging
            command = GetPagingCommand(command, pagingSkipCount, pagingTakeCount);
            #endregion            
            return command;
        }

        public Command GetCountCommand(string[] searchColumns, string[] searchOperators, object[] searchValues, string[] logicalOperators)
        {
            Command command = GetCountCommand(this.tableName);
            #region condition
            Command condition = GetConditionCommand(searchColumns, searchOperators, searchValues, logicalOperators);
            if ((condition != null) && (!string.IsNullOrEmpty(condition.CommandText)))
            {
                command = AppendAdditionCommand(command, condition);
            }
            #endregion
            return command;
        }
    }
}
