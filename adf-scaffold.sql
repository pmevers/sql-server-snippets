-- Scaffold ADF functions. This is useful when you need to use data flows in Azure Data Factory and need to write functions involving a large number of columns such as hashes.

DROP TABLE IF EXISTS #adf_string

-- Declare variables
  DECLARE @table_schema sysname
  DECLARE @table sysname
  DECLARE @adf_function nvarchar(100)
  DECLARE @column_with_wrappers nvarchar(max) = '', @base_function nvarchar(50), @base_function_suffix nvarchar(10)
  DECLARE @closing_statement nvarchar(255)
  
  --Set variables, or pass through stored procedure
  SET @table = 'Your Table'
  SET @table_schema = 'Your Schema'
  SET @adf_function = 'iif({' -- Include opening wrapper
  SET @closing_statement = '}=='''','''')' --suffix applied to column
  SET @base_function = 'mdf(toBase64(' --prefix if needed
  SET @base_function_suffix = '))' --suffix if needed
  
-- Specify Table Schema, ADF Function, Function Wrappers
  SELECT
    ROW_NUMBER() OVER (PARTITION BY COLUMN_NAME ORDER BY (SELECT NULL)) AS RowNum,
    COLUMN_NAME as ColumnName
    INTO #adf_string
    FROM [Insert your DB].INFORMATION_SCHEMA.COLUMNS
    WHERE TABLE_SCHEMA = @table_schema AND TABLE_NAME = @table
-- Additional Filters if Needed
  AND COLUMN_NAME NOT IN ('Your', 'Unwanted', 'Columns')
  
--Initialize counter with dynamic bounds based on number of columns
  DECLARE @Counter int, @MaxRnum Int
  SELECT @Counter = min(RowNum), @MaxRnum = max(RowNum)
  FROM #adf_string
  
--Loop through each column name and append to output
  WHILE (@Counter is not null and @Counter <= @MaxRNum)
    BEGIN
      SELECT @columns_with_wrappers = @column_with_wrappers -- or previous column(s)
        + @adf_function
        + ColumnName
        + @closing_statement
        + '+'
      FROM #adf_string
      WHERE @Counter = RowNum
      SET @Counter = @Counter + 1
     END

-- Remove trailing '+'
  SELECT @columns_with_wrappers = @base_function + rtrim(@columns_with_wrappers, '+') + @base_function_suffix
  
-- Return string
SELECT(@columns_with_wrappers)
      
