<?php
declare(strict_types = 1);

namespace Simbiat\Database;

use Simbiat\CuteBytes;
use function is_string, count, in_array;

/**
 * Useful semantic wrappers to manage your database
 */
final class Manage extends Query
{
    
    /**
     * Check if table(s) exist(s)
     *
     * @param string|array $table  Table name(s)
     * @param string|array $schema Optional (but recommended) schema name(s)
     *
     * @return int Number of table(s) found
     */
    public static function checkTable(string|array $table, string|array $schema = ''): int
    {
        try {
            #Adjust the query depending on whether schema is set
            if (empty($schema)) {
                $query = 'SELECT COUNT(*) as `count` FROM `information_schema`.`TABLES` WHERE `TABLE_NAME` IN(:table);';
                $bindings = [
                    ':table' =>
                        [
                            $table,
                            is_string($table) ? 'string' : 'in',
                            'string'
                        ]
                ];
            } else {
                $query = 'SELECT COUNT(*) as `count` FROM `information_schema`.`TABLES` WHERE `TABLE_NAME` IN(:table) AND `TABLE_SCHEMA` IN(:schema);';
                $bindings = [
                    ':table' =>
                        [
                            $table,
                            is_string($table) ? 'string' : 'in',
                            'string'
                        ],
                    ':schema' =>
                        [
                            $schema,
                            is_string($schema) ? 'string' : 'in',
                            'string'
                        ]
                ];
            }
            return Select::count($query, $bindings);
        } catch (\Throwable $e) {
            throw new \RuntimeException('Failed to check if table exists with `'.$e->getMessage().'`', 0, $e);
        }
    }
    
    /**
     * Get column data type
     *
     * @param string $table  Table name
     * @param string $column Column name
     * @param string $schema Optional (but recommended) schema name
     *
     * @return string
     */
    public static function getColumnType(string $table, string $column, string $schema = ''): string
    {
        try {
            if (empty($schema)) {
                $query = 'SELECT `DATA_TYPE` FROM `information_schema`.`COLUMNS` WHERE `TABLE_NAME`=:table AND `COLUMN_NAME`=:column LIMIT 1;';
                $bindings = [
                    ':table' => $table,
                    ':column' => $column
                ];
            } else {
                $query = 'SELECT `DATA_TYPE` FROM `information_schema`.`COLUMNS` WHERE `TABLE_SCHEMA`=:schema AND `TABLE_NAME`=:table AND `COLUMN_NAME`=:column;';
                $bindings = [
                    ':table' => $table,
                    ':column' => $column,
                    ':schema' => $schema
                ];
            }
            return Select::selectValue($query, $bindings);
        } catch (\Throwable $e) {
            throw new \RuntimeException('Failed to check if table exists with `'.$e->getMessage().'`', 0, $e);
        }
    }
    
    /**
     * Get column description
     *
     * @param string $table  Table name
     * @param string $column Column name
     * @param string $schema Optional (but recommended) schema name
     *
     * @return string
     */
    public static function getColumnDescription(string $table, string $column, string $schema = ''): string
    {
        try {
            if (empty($schema)) {
                $query = 'SELECT `COLUMN_COMMENT` FROM `information_schema`.`COLUMNS` WHERE `TABLE_NAME`=:table AND `COLUMN_NAME`=:column LIMIT 1;';
                $bindings = [
                    ':table' => $table,
                    ':column' => $column
                ];
            } else {
                $query = 'SELECT `COLUMN_COMMENT` FROM `information_schema`.`COLUMNS` WHERE `TABLE_SCHEMA`=:schema AND `TABLE_NAME`=:table AND `COLUMN_NAME`=:column;';
                $bindings = [
                    ':table' => $table,
                    ':column' => $column,
                    ':schema' => $schema
                ];
            }
            return Select::selectValue($query, $bindings);
        } catch (\Throwable $e) {
            throw new \RuntimeException('Failed to check if table exists with `'.$e->getMessage().'`', 0, $e);
        }
    }
    
    /**
     * Check if a column is nullable
     *
     * @param string $table  Table name
     * @param string $column Column name
     * @param string $schema Optional (but recommended) schema name
     *
     * @return bool
     */
    public static function isNullable(string $table, string $column, string $schema = ''): bool
    {
        try {
            if (empty($schema)) {
                $query = 'SELECT `IS_NULLABLE` FROM `information_schema`.`COLUMNS` WHERE `TABLE_NAME`=:table AND `COLUMN_NAME`=:column LIMIT 1;';
                $bindings = [
                    ':table' => $table,
                    ':column' => $column
                ];
            } else {
                $query = 'SELECT `IS_NULLABLE` FROM `information_schema`.`COLUMNS` WHERE `TABLE_SCHEMA`=:schema AND `TABLE_NAME`=:table AND `COLUMN_NAME`=:column;';
                $bindings = [
                    ':table' => $table,
                    ':column' => $column,
                    ':schema' => $schema
                ];
            }
            $result = Select::selectValue($query, $bindings);
            return preg_match('/^YES$/ui', $result) === 1;
        } catch (\Throwable $e) {
            throw new \RuntimeException('Failed to check if table exists with `'.$e->getMessage().'`', 0, $e);
        }
    }
    
    /**
     * Check if Foreign Key name exists
     *
     * @param string $table  Table name
     * @param string $fk     Foreign Key name
     * @param string $schema Optional (but recommended) schema name
     *
     * @return bool
     */
    public static function checkFK(string $table, string $fk, string $schema = ''): bool
    {
        try {
            if (empty($schema)) {
                $query = 'SELECT `CONSTRAINT_NAME` FROM `information_schema`.`TABLE_CONSTRAINTS` WHERE `TABLE_NAME`=:table AND `CONSTRAINT_NAME`=:fk LIMIT 1;';
                $bindings = [
                    ':table' => $table,
                    ':fk' => $fk
                ];
            } else {
                $query = 'SELECT `CONSTRAINT_NAME` FROM `information_schema`.`TABLE_CONSTRAINTS` WHERE `TABLE_SCHEMA`=:schema AND `TABLE_NAME`=:table AND `CONSTRAINT_NAME`=:fk;';
                $bindings = [
                    ':table' => $table,
                    ':fk' => $fk,
                    ':schema' => $schema
                ];
            }
            return Select::check($query, $bindings);
        } catch (\Throwable $e) {
            throw new \RuntimeException('Failed to check if table exists with `'.$e->getMessage().'`', 0, $e);
        }
    }
    
    /**
     * Check if a column exists in a table
     *
     * @param string $table  Table name
     * @param string $column Column to check
     * @param string $schema Optional (but recommended) schema name
     *
     * @return bool
     */
    public static function checkColumn(string $table, string $column, string $schema = ''): bool
    {
        try {
            #Adjust the query depending on whether schema is set
            if (empty($schema)) {
                $query = 'SELECT `COLUMN_NAME` FROM `information_schema`.`COLUMNS` WHERE `TABLE_NAME` = :table AND `COLUMN_NAME` = :column;';
                $bindings = [':table' => $table, ':column' => $column];
            } else {
                $query = 'SELECT `COLUMN_NAME` FROM `information_schema`.`COLUMNS` WHERE `TABLE_NAME` = :table AND `COLUMN_NAME` = :column AND `TABLE_SCHEMA` = :schema;';
                $bindings = [':table' => $table, ':column' => $column, ':schema' => $schema];
            }
            return Select::check($query, $bindings);
        } catch (\Throwable $e) {
            throw new \RuntimeException('Failed to check if column exists with `'.$e->getMessage().'`', 0, $e);
        }
    }
    
    /**
     * Function to get a list of all tables for a schema in order, where first you have tables without dependencies (no foreign keys), and then tables that are dependent on tables that has come before. This is useful if you want to dump backups in a specific order so that you can then restore the data without disabling foreign keys.
     * Only for MySQL/MariaDB
     *
     * @param string $schema Optional name of the schema to limit to
     * @param bool   $bySize
     *
     * @return array
     */
    public static function showOrderedTables(string $schema = '', bool $bySize = false): array
    {
        #This is the list of the tables that we will return in the end
        $tablesOrderedFull = [];
        #This is the list of the same tables, but where every element is a string of format `schema`.`table`. Used for array search only
        $tablesNamesOnly = [];
        #Get all tables except standard system ones and also order them by size
        $tablesRaw = Select::selectAll('SELECT `TABLE_SCHEMA` as `schema`, `TABLE_NAME` as `table`'.($bySize ? ', (DATA_LENGTH+INDEX_LENGTH) as `size`' : '').' FROM INFORMATION_SCHEMA.TABLES WHERE `TABLE_SCHEMA` NOT IN (\'information_schema\', \'performance_schema\', \'mysql\', \'sys\', \'test\')'.(empty($schema) ? '' : ' AND `TABLE_SCHEMA`=:schema').' ORDER BY '.($bySize ? '(DATA_LENGTH+INDEX_LENGTH), ' : '').'`TABLE_SCHEMA`, `TABLE_NAME`;', (empty($schema) ? [] : [':schema' => [$schema, 'string']]));
        #Get dependencies for each table
        foreach ($tablesRaw as $key => $table) {
            $table['dependencies'] = self::selectAllDependencies($table['schema'], $table['table']);
            if (count($table['dependencies']) === 0) {
                #Add this to the ordered list right away if we have no dependencies
                $tablesOrderedFull[] = $table;
                $tablesNamesOnly[] = '`'.$table['schema'].'`.`'.$table['table'].'`';
                unset($tablesRaw[$key]);
            } else {
                #Update the raw list with dependencies to use further
                $tablesRaw[$key] = $table;
            }
        }
        #Check if we have any cyclic references among the remaining tables
        if (self::checkCyclicForeignKeys($tablesRaw)) {
            #Throw an error, because with cyclic references there is no way to determine the order at all
            throw new \PDOException('Cyclic foreign key references detected.');
        }
        #While is used because when we reach the end in first run, we may still have items left in the array
        while (count($tablesRaw) > 0) {
            foreach ($tablesRaw as $key => $table) {
                #Check if the table is already present in the ordered list
                foreach ($table['dependencies'] as $dKey => $dependency) {
                    #If a dependency is not already present in the list of tables - go to the next table
                    if (!in_array($dependency, $tablesNamesOnly, true)) {
                        continue 2;
                    }
                    #Remove dependency
                    unset($tablesRaw[$key]['dependencies'][$dKey]);
                }
                #If we are here, all dependencies are already in the list, so we can add the current table to the list, as well
                $tablesOrderedFull[] = $table;
                $tablesNamesOnly[] = '`'.$table['schema'].'`.`'.$table['table'].'`';
                unset($tablesRaw[$key]);
            }
        }
        #Convert size to human bytes if the respective function exists
        if ($bySize && method_exists(CuteBytes::class, 'bytes')) {
            foreach ($tablesOrderedFull as $key => $table) {
                $tablesOrderedFull[$key]['size'] = CuteBytes::bytes($table['size'], 1024);
            }
        }
        return $tablesOrderedFull;
    }
    
    /**
     * This function allows you to check for cyclic foreign keys when 2 (or more) tables depend on each other.
     * This is considered bad practice even with nullable columns, but you may easily miss them as your database grows, especially if you have chains of 3 or more tables.
     * This will not return the specific FKs you need to deal with, but rather just a list of tables referencing tables that refer to the initial ones.
     * You will need to analyze the references yourself to "untangle" them properly.
     * You can pass a prepared list of tables with a format of ['schema' => 'schema_name', 'table' => 'table_name'].
     * This array can also include key 'dependencies', which should be an array of values like '`schema_name`.`table_name`',
     * Only for MySQL/MariaDB
     *
     * @param array|null $tables Optional list of tables in `schema.table` format. If none is provided, will first get a list of all tables available.
     *
     * @return bool
     */
    public static function checkCyclicForeignKeys(?array $tables = null): bool
    {
        #Unfortunately, I was not able to make things work with just 1 query with a recursive sub-query, so doing things in 2 steps.
        #The first step is to get all tables that have FKs but exclude those that refer themselves
        if ($tables === null) {
            $tables = Select::selectAll('SELECT `TABLE_SCHEMA` AS `schema`, `TABLE_NAME` AS `table` FROM `information_schema`.`KEY_COLUMN_USAGE` WHERE `REFERENCED_TABLE_SCHEMA` IS NOT NULL AND CONCAT(`REFERENCED_TABLE_SCHEMA`, \'.\', `REFERENCED_TABLE_NAME`) != CONCAT(`TABLE_SCHEMA`, \'.\', `TABLE_NAME`) GROUP BY `TABLE_SCHEMA`, `TABLE_NAME`;');
        }
        foreach ($tables as $key => $table) {
            #For each table get their recursive list of dependencies, if not set in the prepared array
            if (!isset($table['dependencies'])) {
                $table['dependencies'] = self::selectAllDependencies($table['schema'], $table['table']);
            }
            #Check if the dependency list has the table itself
            if (in_array('`'.$table['schema'].'`.`'.$table['table'].'`', $table['dependencies'], true)) {
                #Update the list (only really needed if we did not have a prepared list of tables from the start)
                $tables[$key] = $table;
            } else {
                #No cyclic references - remove the table from the list
                unset($tables[$key]);
            }
        }
        return (count($tables) > 0);
    }
    
    /**
     * Function to recursively get all dependencies (foreign keys) of a table.
     * Only for MySQL/MariaDB
     *
     * @param string $schema Schema name
     * @param string $table  Table name
     *
     * @return array
     */
    public static function selectAllDependencies(string $schema, string $table): array
    {
        #We are using backticks when comparing the schemas and tables, since that will definitely avoid any matches due to dots in names
        return Select::selectColumn('
                 WITH RECURSIVE `DependencyTree` AS (
                    SELECT
                        CONCAT(\'`\', `REFERENCED_TABLE_SCHEMA`, \'`.`\', `REFERENCED_TABLE_NAME`, \'`\') AS `dependency`
                    FROM
                        `INFORMATION_SCHEMA`.`KEY_COLUMN_USAGE`
                    WHERE
                        `TABLE_SCHEMA` = :schema
                        AND `TABLE_NAME` = :table
                        AND `REFERENCED_TABLE_NAME` IS NOT NULL
                        AND CONCAT(\'`\', `TABLE_SCHEMA`, \'`.`\', `TABLE_NAME`, \'`\') != CONCAT(\'`\', `REFERENCED_TABLE_SCHEMA`, \'`.`\', `REFERENCED_TABLE_NAME`, \'`\')
                    UNION ALL
                    SELECT
                        CONCAT(\'`\', `kcu`.`REFERENCED_TABLE_SCHEMA`, \'`.`\', `kcu`.`REFERENCED_TABLE_NAME`, \'`\') AS `dependency`
                    FROM
                        `INFORMATION_SCHEMA`.`KEY_COLUMN_USAGE` AS `kcu`
                    INNER JOIN
                        `DependencyTree` AS `dt` ON CONCAT(\'`\', `kcu`.`TABLE_SCHEMA`, \'`.`\', `kcu`.`TABLE_NAME`, \'`\') = `dt`.`dependency`
                    WHERE
                        `REFERENCED_TABLE_NAME` IS NOT NULL
                        AND CONCAT(\'`\', `TABLE_SCHEMA`, \'`.`\', `TABLE_NAME`, \'`\') != CONCAT(\'`\', `REFERENCED_TABLE_SCHEMA`, \'`.`\', `REFERENCED_TABLE_NAME`, \'`\')
                )
                SELECT DISTINCT `dependency`
                    FROM `DependencyTree`;',
            [':schema' => $schema, ':table' => $table]
        );
    }
    
    /**
     * Function to restore ROW_FORMAT value to table definition.
     * MySQL/MariaDB may now have ROW_FORMAT in SHOW CREATE TABLE output or have a value, which is different from the current one. This function amends that.
     * Due to SHOW CREATE TABLE being special, we can't use it as a sub-query, so need to do 2 queries instead.
     * Only for MySQL/MariaDB
     *
     * @param string $schema      Schema name.
     * @param string $table       Table name.
     * @param bool   $noIncrement Remove the `AUTO_INCREMENT=X` table option. Column attribute will still be present.
     * @param bool   $ifNotExist  Add the `IF NOT EXISTS` clause to the resulting definition.
     * @param bool   $addUse      Add the `USE` statement before the `CREATE` statement.
     *
     * @return string|null
     */
    public static function showCreateTable(string $schema, string $table, bool $noIncrement = true, bool $ifNotExist = false, bool $addUse = false): ?string
    {
        #Get the original create function
        $create = Select::selectValue('SHOW CREATE TABLE `'.$schema.'`.`'.$table.'`;', [], 1);
        #Add semicolon for consistency
        if (!str_ends_with(';', $create)) {
            $create .= ';';
        }
        #Get current ROW_FORMAT value
        $rowFormat = Select::selectValue('SELECT `ROW_FORMAT` FROM `information_schema`.`TABLES` WHERE `TABLE_SCHEMA`=:schema AND `TABLE_NAME`=:table;', [':schema' => $schema, ':table' => $table]);
        #Check the value against create statement
        if (preg_match('/ROW_FORMAT='.$rowFormat.'/ui', $create) !== 1) {
            #Value differs or missing
            if (preg_match('/ROW_FORMAT=/ui', $create) === 1) {
                #If ROW_FORMAT is already present, we need to replace it
                $create = preg_replace('/ROW_FORMAT=[^ ]+/ui', 'ROW_FORMAT='.$rowFormat.';', $create);
            } else {
                #Else we need to add it to the end
                $create = preg_replace('/;$/u', ' ROW_FORMAT='.$rowFormat.';', $create);
            }
        }
        if ($noIncrement) {
            $create = preg_replace('/(\s* AUTO_INCREMENT=\d+)/ui', '', $create);
        }
        if ($ifNotExist) {
            $create = preg_replace('/^CREATE TABLE/ui', /** @lang text */ 'CREATE TABLE IF NOT EXISTS', $create);
        }
        if ($addUse) {
            $create = 'USE `'.$schema.'`;'.PHP_EOL.$create;
        }
        #Return result
        return $create;
    }
}