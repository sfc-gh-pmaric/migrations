USE DATABASE CARS_DEMO_DB; 
USE SCHEMA SPEED_TESTS; 
create STAGE if not exists "CARS_DEMO_DB"."SPEED_TESTS"."STAGE1"     
  FILE_FORMAT = ( 
  TYPE = 'CSV' 
  BINARY_FORMAT = 'HEX' 
  COMPRESSION = 'AUTO' 
  DATE_FORMAT = 'AUTO' 
  EMPTY_FIELD_AS_NULL = true 
  ENCODING = 'UTF8' 
  ERROR_ON_COLUMN_COUNT_MISMATCH = true 
  ESCAPE = 'NONE' 
  ESCAPE_UNENCLOSED_FIELD = '\\' 
  FIELD_DELIMITER = ',' 
  FIELD_OPTIONALLY_ENCLOSED_BY = 'NONE' 
 NULL_IF = ('\\N') 
  RECORD_DELIMITER = '\n' 
  REPLACE_INVALID_CHARACTERS = false 
  SKIP_BLANK_LINES = false 
  SKIP_BYTE_ORDER_MARK = true 
  SKIP_HEADER = 0 
  TIMESTAMP_FORMAT = 'AUTO' 
  TIME_FORMAT = 'AUTO' 
  TRIM_SPACE = false 
  VALIDATE_UTF8 = true  
) 
  COPY_OPTIONS = ( 
  ENFORCE_LENGTH = true 
  FORCE = false 
  ON_ERROR = 'ABORT_STATEMENT' 
  PURGE = false 
  RETURN_FAILED_ONLY = false 
  TRUNCATECOLUMNS = false  
) COMMENT = '';
create STAGE if not exists "CARS_DEMO_DB"."SPEED_TESTS"."STAGE2"     
  FILE_FORMAT = ( 
  TYPE = 'CSV' 
  BINARY_FORMAT = 'HEX' 
  COMPRESSION = 'AUTO' 
  DATE_FORMAT = 'AUTO' 
  EMPTY_FIELD_AS_NULL = true 
  ENCODING = 'UTF8' 
  ERROR_ON_COLUMN_COUNT_MISMATCH = true 
  ESCAPE = 'NONE' 
  ESCAPE_UNENCLOSED_FIELD = '\\' 
  FIELD_DELIMITER = ',' 
  FIELD_OPTIONALLY_ENCLOSED_BY = 'NONE' 
 NULL_IF = ('\\N') 
  RECORD_DELIMITER = '\n' 
  REPLACE_INVALID_CHARACTERS = false 
  SKIP_BLANK_LINES = false 
  SKIP_BYTE_ORDER_MARK = true 
  SKIP_HEADER = 0 
  TIMESTAMP_FORMAT = 'AUTO' 
  TIME_FORMAT = 'AUTO' 
  TRIM_SPACE = false 
  VALIDATE_UTF8 = true  
) 
  COPY_OPTIONS = ( 
  ENFORCE_LENGTH = true 
  FORCE = false 
  ON_ERROR = 'ABORT_STATEMENT' 
  PURGE = false 
  RETURN_FAILED_ONLY = false 
  TRUNCATECOLUMNS = false  
) COMMENT = '';
