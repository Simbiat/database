<?php
declare(strict_types = 1);

namespace Simbiat\Database;

use Simbiat\SandClock;

use function is_string, count, in_array, is_array;

/**
 * Query the database
 */
class Controller
{
    #List of functions, that may return rows
    public const array selects = [
        'SELECT', 'SHOW', 'HANDLER', 'ANALYZE', 'CHECK', 'DESCRIBE', 'DESC', 'EXPLAIN', 'HELP'
    ];
    /**
     * @var int Number of queries ran. Static for convenience, in case object gets destroyed, but you still want to get total number
     */
    public static int $queries = 0;
    private ?\PDO $dbh;
    /**
     * @var bool Debug mode
     */
    public bool $debug = false;
    /**
     * @var int Maximum time (in seconds) for the query (for `set_time_limit`)
     */
    private int $maxRunTime = 3600; #in seconds
    /**
     * @var int Number of times to retry in case of deadlock
     */
    private int $maxTries = 5;
    /**
     * @var int Time (in seconds) to wait between retries in case of deadlock
     */
    private int $sleep = 5;
    /**
     * @var mixed Result of the last query
     */
    private mixed $result = null;
    private null|string|false $lastId = null;
    
    public function __construct()
    {
        $this->dbh = Pool::openConnection();
    }
    
    /**
     * Run SQL query
     *
     * @param string|array $queries        - query/queries to run
     * @param array        $bindings       - global bindings, that need to be applied to all queries
     * @param int          $fetch_style    - FETCH type used by SELECT queries. Applicable only if 1 query is sent
     * @param mixed|null   $fetch_argument - fetch mode for PDO
     * @param array        $ctor_args      - constructorArgs for fetchAll PDO function
     * @param bool         $transaction    - flag whether to use TRANSACTION mode. TRUE by default to allow more consistency
     *
     * @return bool
     */
    public function query(string|array $queries, array $bindings = [], int $fetch_style = \PDO::FETCH_ASSOC, int|string|object|null $fetch_argument = NULL, array $ctor_args = [], bool $transaction = true): bool
    {
        #Reset lastID
        $this->lastId = null;
        #Check if query string was sent
        if (is_string($queries)) {
            if (preg_match('/^\s*$/i', $queries) === 1) {
                throw new \UnexpectedValueException('Query is an empty string.');
            }
            #Convert to array
            $queries = [[$queries, $bindings]];
        } else {
            #Ensure integer keys
            $queries = array_values($queries);
            #Iterrate over array to merge binding
            foreach ($queries as $key => $query) {
                #Ensure integer keys
                if (is_string($query)) {
                    $query = [0 => $query, 1 => []];
                }
                $queries[$key] = array_values($query);
                #Check if query is a string
                if (!is_string($queries[$key][0])) {
                    #Exit earlier for speed
                    throw new \UnexpectedValueException('Query #'.$key.' is not a string.');
                } else {
                    if (preg_match('/^\s*$/i', $queries[$key][0]) === 1) {
                        throw new \UnexpectedValueException('Query #'.$key.' is an empty string.');
                    }
                }
                #Merge bindings
                $queries[$key][1] = array_merge($queries[$key][1] ?? [], $bindings);
            }
        }
        #Remove any SELECT queries and comments if more than 1 query is sent
        if (count($queries) > 1) {
            foreach ($queries as $key => $query) {
                #Check if query is SELECT
                if ($this->isSelect($query[0], false)) {
                    unset($queries[$key]);
                    continue;
                }
                #Check if query is a comment
                if (preg_match('/^\s*(--|#|\/\*).*$/', $query[0]) === 1) {
                    unset($queries[$key]);
                }
            }
        }
        #Check if array of queries is empty
        if (empty($queries)) {
            #Issue a notice
            trigger_error('No queries were provided to `query()` function');
            #Do not consider this an "error" by default and return `true`
            return true;
        }
        #Flag for SELECT, used as sort of "cache" instead of counting values every time
        $select = false;
        #If we have just 1 query, which is a SELECT - disable transaction
        if ((count($queries) === 1) && preg_match('/^\s*\(*'.implode('|', self::selects).'/mi', $queries[0][0]) === 1) {
            $select = true;
            $transaction = false;
        }
        #Check if we are running a SELECT
        if (!$select) {
            #If not - use $result as counter for number of affected rows and reset it before run
            $this->result = 0;
        }
        #Set counter for tries
        $try = 0;
        do {
            #Suppressing because we want to standardize error handling for this part
            /** @noinspection BadExceptionsProcessingInspection */
            try {
                #Indicate actual try
                $try++;
                #Initiate transaction, if we are using it
                if ($transaction) {
                    $this->dbh->beginTransaction();
                }
                #Loop through queries
                foreach ($queries as $key => $query) {
                    #Reset variables
                    $sql = null;
                    $currentBindings = null;
                    $currentKey = $key;
                    #Prepare query
                    if ($this->dbh->getAttribute(\PDO::ATTR_DRIVER_NAME) === 'mysql') {
                        #Force buffered query for MySQL
                        $sql = $this->dbh->prepare($query[0], [\PDO::MYSQL_ATTR_USE_BUFFERED_QUERY => true]);
                    } else {
                        $sql = $this->dbh->prepare($query[0]);
                    }
                    #Bind values, if any
                    if (!empty($query[1])) {
                        $currentBindings = $this->cleanBindings($query[0], $query[1]);
                        $sql = $this->binding($sql, $currentBindings);
                    }
                    #Increasing time limit for potentially long operations (like optimize)
                    set_time_limit($this->maxRunTime);
                    #Increase the number of queries
                    self::$queries++;
                    #Execute the query
                    $sql->execute();
                    #If debug is enabled dump PDO details
                    if ($this->debug) {
                        $sql->debugDumpParams();
                        ob_flush();
                        flush();
                    }
                    if ($select) {
                        #Adjust fetching mode
                        if ($fetch_argument === 'row') {
                            $this->result = $sql->fetchAll($fetch_style);
                            if (isset($this->result[0])) {
                                $this->result = $this->result[0];
                            }
                        } elseif (in_array($fetch_style, [\PDO::FETCH_COLUMN, \PDO::FETCH_FUNC, \PDO::FETCH_INTO], true)) {
                            $this->result = $sql->fetchAll($fetch_style, $fetch_argument);
                        } elseif ($fetch_style === \PDO::FETCH_CLASS) {
                            $this->result = $sql->fetchAll($fetch_style, $fetch_argument, $ctor_args);
                        } else {
                            $this->result = $sql->fetchAll($fetch_style);
                        }
                    } else {
                        #Increase counter of affected rows (inserted, deleted, updated)
                        $this->result += $sql->rowCount();
                    }
                    #Explicitely close pointer to release resources
                    $sql->closeCursor();
                    #Remove the query from the bulk, if not using transaction mode, to avoid repeating of commands
                    #Not sure why PHP Storm complains about this line
                    /** @noinspection PhpConditionAlreadyCheckedInspection */
                    if (!$transaction) {
                        unset($queries[$key]);
                    }
                }
                #Try to get last ID (if we had any inserts with auto increment
                try {
                    $this->lastId = $this->dbh->lastInsertId();
                } catch (\Throwable) {
                    #Either the function is not supported by driver or it requires a sequence name.
                    #Since this class is meant to be universal, I do not see a good way to support sequence name at the time of writing.
                    $this->lastId = false;
                }
                #Initiate transaction, if we are using it
                if ($transaction && $this->dbh->inTransaction()) {
                    $this->dbh->commit();
                }
                return true;
            } catch (\Throwable $e) {
                $errMessage = $e->getMessage().$e->getTraceAsString();
                #We can get here without $sql being set, when initiating transaction fails
                /** @noinspection PhpConditionAlreadyCheckedInspection */
                if (isset($sql) && $this->debug) {
                    $sql->debugDumpParams();
                    echo $errMessage;
                    ob_flush();
                    flush();
                }
                #Check if it's a deadlock. Unbuffered queries are not deadlock, but practice showed, that in some cases this error is thrown when there is a lock on resources, and not really an issue with (un)buffered queries. Retrying may help in those cases.
                #We can get here without $sql being set, when initiating transaction fails
                /** @noinspection PhpConditionAlreadyCheckedInspection */
                if (isset($sql) && ($sql->errorCode() === '40001' || preg_match('/(deadlock|try restarting transaction|Cannot execute queries while other unbuffered queries are active)/mi', $errMessage) === 1)) {
                    $deadlock = true;
                } else {
                    $deadlock = false;
                    #Set error message
                    if (!isset($currentKey)) {
                        $errMessage = 'Failed to start or end transaction';
                    } else {
                        try {
                            $errMessage = 'Failed to run query `'.$queries[$currentKey][0].'`'.(!empty($currentBindings) ? ' with following bindings: '.json_encode($currentBindings, JSON_THROW_ON_ERROR) : '');
                        } catch (\JsonException) {
                            $errMessage = 'Failed to run query `'.$queries[$currentKey][0].'`'.(!empty($currentBindings) ? ' with following bindings: `Failed to JSON Encode bindings`' : '');
                        }
                    }
                }
                #We can get here without $sql being set, when initiating transaction fails
                /** @noinspection PhpConditionAlreadyCheckedInspection */
                if (isset($sql)) {
                    #Ensure pointer is closed
                    try {
                        $sql->closeCursor();
                    } catch (\Throwable) {
                        #Do nothing, most likely fails due to non-existent cursor.
                    }
                }
                if ($this->dbh->inTransaction()) {
                    $this->dbh->rollBack();
                    if (!$deadlock) {
                        throw new \RuntimeException($errMessage, 0, $e);
                    }
                }
                #If deadlock - sleep and then retry
                if ($deadlock) {
                    sleep($this->sleep);
                    continue;
                }
                throw new \RuntimeException($errMessage, 0, $e);
            }
        } while ($try <= $this->maxTries);
        throw new \RuntimeException('Deadlock encountered for set maximum of '.$this->maxTries.' tries.');
    }
    
    /**
     * Function to clean extra bindings from a list, if they are not present in the query itself
     * @param string $sql      Query to process
     * @param array  $bindings List of bindings
     *
     * @return array
     */
    private function cleanBindings(string $sql, array $bindings): array
    {
        foreach ($bindings as $binding => $value) {
            if (!str_contains($sql, $binding)) {
                unset($bindings[$binding]);
            }
        }
        return $bindings;
    }
    
    /**
     * Function mainly for convenience and some types enforcing, which sometimes 'fail' in PDO itself
     * @param \PDOStatement $sql      Query to process
     * @param array         $bindings List of bindings
     *
     * @return \PDOStatement
     */
    private function binding(\PDOStatement $sql, array $bindings = []): \PDOStatement
    {
        try {
            foreach ($bindings as $binding => $value) {
                if (is_array($value)) {
                    #Handle malformed UTF
                    if (is_string($value[0])) {
                        $value[0] = mb_scrub($value[0], 'UTF-8');
                    }
                    if (!isset($value[1]) || !is_string($value[1])) {
                        $value[1] = '';
                    }
                    switch (mb_strtolower($value[1], 'UTF-8')) {
                        case 'date':
                            $sql->bindValue($binding, SandClock::format($value[0], 'Y-m-d'));
                            break;
                        case 'time':
                            $sql->bindValue($binding, SandClock::format($value[0], 'H:i:s.u'));
                            break;
                        case 'datetime':
                            $sql->bindValue($binding, SandClock::format($value[0]));
                            break;
                        case 'bool':
                        case 'boolean':
                            $sql->bindValue($binding, (bool)$value[0], \PDO::PARAM_BOOL);
                            break;
                        case 'null':
                            $sql->bindValue($binding, null, \PDO::PARAM_NULL);
                            break;
                        case 'int':
                        case 'integer':
                        case 'number':
                        case 'limit':
                        case 'offset':
                            $sql->bindValue($binding, (int)$value[0], \PDO::PARAM_INT);
                            break;
                        case 'str':
                        case 'string':
                        case 'text':
                        case 'float':
                        case 'varchar':
                        case 'varchar2':
                            $sql->bindValue($binding, (string)$value[0]);
                            break;
                        case 'match':
                            #Same as string, but for MATCH operator, when your string can have special characters, that will break the query
                            $sql->bindValue($binding, $this->match((string)$value[0]));
                            break;
                        case 'like':
                            #Same as string, but wrapped in % for LIKE '%string%'
                            $sql->bindValue($binding, '%'.$value[0].'%');
                            break;
                        case 'lob':
                        case 'large':
                        case 'object':
                        case 'blob':
                            $sql->bindParam($binding, $value[0], \PDO::PARAM_LOB, strlen($value[0]));
                            break;
                        default:
                            if (\is_int($value[1])) {
                                $sql->bindValue($binding, $value[0], $value[1]);
                            } else {
                                $sql->bindValue($binding, (string)$value[0]);
                            }
                    }
                } else {
                    #Handle malformed UTF
                    if (is_string($value)) {
                        $value = mb_scrub($value, 'UTF-8');
                    }
                    $sql->bindValue($binding, $value);
                }
            }
        } catch (\Throwable $exception) {
            $errMessage = 'Failed to bind variable `'.$binding.'`';
            if (is_array($value)) {
                $errMessage .= ' of type `'.$value[1].'` with value `'.$value[0].'`';
            } else {
                $errMessage .= ' with value `'.$value.'`';
            }
            throw new \RuntimeException($errMessage, 0, $exception);
        }
        return $sql;
    }
    
    /** Helper function to prepare string binding for MATCH in FULLTEXT search
     * @param string $string
     *
     * @return string
     */
    private function match(string $string): string
    {
        #Trim first
        $newValue = preg_replace('/^[\p{Z}\h\v\r\n]+|[\p{Z}\h\v\r\n]+$/u', '', $string);
        #Remove all symbols except allowed operators and space. @distance is not included, since it's unlikely a human will be using it through UI form
        $newValue = preg_replace('/[^\p{L}\p{N}_+\-<>~()"* ]/u', '', $newValue);
        #Remove all operators, that can only precede a text and that are not preceded by either beginning of string or space
        $newValue = preg_replace('/(?<!^| )[+\-<>~]/u', '', $newValue);
        #Remove all double quotes and asterisks, that are not preceded by either beginning of string, letter, number or space
        $newValue = preg_replace('/(?<![\p{L}\p{N}_ ]|^)[*"]/u', '', $newValue);
        #Remove all double quotes and asterisks, that are inside text
        $newValue = preg_replace('/([\p{L}\p{N}_])([*"])([\p{L}\p{N}_])/u', '', $newValue);
        #Remove all opening parenthesis which are not preceded by beginning of string or space
        $newValue = preg_replace('/(?<!^| )\(/u', '', $newValue);
        #Remove all closing parenthesis which are not preceded by beginning of string or space or are not followed by end of string or space
        $newValue = preg_replace('/(?<![\p{L}\p{N}_])\)|\)(?! |$)/u', '', $newValue);
        #Remove all double quotes if the count is not even
        if (mb_substr_count($newValue, '"', 'UTF-8') % 2 !== 0) {
            $newValue = preg_replace('/"/u', '', $newValue);
        }
        #Remove all parenthesis if count of closing does not match count of opening ones
        if (mb_substr_count($newValue, '(', 'UTF-8') !== mb_substr_count($newValue, ')', 'UTF-8')) {
            $newValue = preg_replace('/[()]/u', '', $newValue);
        }
        #Remove all operators, that can only precede a text and that do not have text after them (at the end of string). Do this for any possible combinations
        $newValue = preg_replace('/[+\-<>~]+$/u', '', $newValue);
        #Remove asterisk operator at the beginning of string
        $newValue = preg_replace('/^\*/u', '', $newValue);
        #Check if the new value is just the set of operators and if it is - set the value to an empty string
        if (preg_match('/^[+\-<>~()"*]+$/u', $newValue)) {
            $newValue = '';
        }
        return $newValue;
    }
    
    ##########################
    #Useful semantic wrappers#
    ##########################
    /**
     * Return full results as multidimensional array (associative by default).
     *
     * @param string $query     Query to run
     * @param array  $bindings  List of bindings
     * @param int    $fetchMode Fetch mode
     *
     * @return array
     */
    public function selectAll(string $query, array $bindings = [], int $fetchMode = \PDO::FETCH_ASSOC): array
    {
        try {
            if ($this->isSelect($query)) {
                self::$queries++;
                if ($this->query($query, $bindings, $fetchMode) && is_array($this->getResult())) {
                    return $this->getResult();
                }
            }
        } catch (\Throwable $e) {
            throw new \RuntimeException('Failed to select rows with `'.$e->getMessage().'`', 0, $e);
        }
        return [];
    }
    
    /**
     * Returns only 1 row from SELECT (essentially LIMIT 1).
     *
     * @param string $query     Query to run
     * @param array  $bindings  List of bindings
     * @param int    $fetchMode Fetch mode
     *
     * @return array
     */
    public function selectRow(string $query, array $bindings = [], int $fetchMode = \PDO::FETCH_ASSOC): array
    {
        try {
            if ($this->isSelect($query)) {
                #Check if the query has a limit (any limit)
                if (preg_match('/\s*LIMIT\s+(\d+\s*,\s*)?\d+\s*;?\s*$/ui', $query) !== 1) {
                    #If it does not - add it. But first we need to remove the final semicolon if any
                    #Need to do this, because otherwise I get 2 matches, which results in 2 entries added in further preg_replace
                    #No idea how to circumvent that
                    $query = preg_replace('/(;?\s*\z)/mui', '', $query);
                    #Add LIMIT
                    $query = preg_replace('/\z/mui', ' LIMIT 0, 1;', $query);
                }
                self::$queries++;
                if ($this->query($query, $bindings, $fetchMode, 'row') && is_array($this->getResult())) {
                    return $this->getResult();
                }
            }
        } catch (\Throwable $e) {
            throw new \RuntimeException('Failed to select row with `'.$e->getMessage().'`', 0, $e);
        }
        return [];
    }
    
    /**
     * Returns column (first by default) even if original SELECT requests for more. Change 3rd parameter accordingly to use another column as key (starting from 0).
     * @param string $query    Query to run
     * @param array  $bindings List of bindings
     * @param int    $column   Number of the column to select
     *
     * @return array
     */
    public function selectColumn(string $query, array $bindings = [], int $column = 0): array
    {
        try {
            if ($this->isSelect($query)) {
                self::$queries++;
                if ($this->query($query, $bindings, \PDO::FETCH_COLUMN, $column) && is_array($this->getResult())) {
                    return $this->getResult();
                }
            }
        } catch (\Throwable $e) {
            throw new \RuntimeException('Failed to select column with `'.$e->getMessage().'`', 0, $e);
        }
        return [];
    }
    
    /**
     * Returns a value directly, instead of array containing that value. Useful for getting specific settings from DB. No return typing, since it may vary, so be careful with that.
     * @param string $query    Query to run
     * @param array  $bindings List of bindings
     * @param int    $column   Number of the column to select
     *
     * @return mixed|null
     */
    public function selectValue(string $query, array $bindings = [], int $column = 0): mixed
    {
        try {
            if ($this->isSelect($query)) {
                self::$queries++;
                if ($this->query($query, $bindings, \PDO::FETCH_COLUMN, $column) && is_array($this->getResult())) {
                    #We always need to take 1st element, since our goal is to return only 1 value
                    return ($this->getResult()[0] ?? NULL);
                }
            }
        } catch (\Throwable $e) {
            throw new \RuntimeException('Failed to select value with `'.$e->getMessage().'`', 0, $e);
        }
        return NULL;
    }
    
    /**
     * Returns key->value pair(s) based on 2 columns. First column (by default) is used as key. Change 3rd parameter accordingly to use another column as key (starting from 0).
     * @param string $query    Query to run
     * @param array  $bindings List of bindings
     * @param int    $column   Number of the column to select
     *
     * @return array
     */
    public function selectPair(string $query, array $bindings = [], int $column = 0): array
    {
        try {
            if ($this->isSelect($query)) {
                self::$queries++;
                if ($this->query($query, $bindings, \PDO::FETCH_KEY_PAIR, $column) && is_array($this->getResult())) {
                    return $this->getResult();
                }
            }
        } catch (\Throwable $e) {
            throw new \RuntimeException('Failed to select pairs with `'.$e->getMessage().'`', 0, $e);
        }
        return [];
    }
    
    /**
     * Returns unique values from a column (first by default). Change 3rd parameter accordingly to use another column as key (starting from 0).
     * @param string $query    Query to run
     * @param array  $bindings List of bindings
     * @param int    $column   Number of the column to select
     *
     * @return array
     */
    public function selectUnique(string $query, array $bindings = [], int $column = 0): array
    {
        try {
            if ($this->isSelect($query)) {
                self::$queries++;
                if ($this->query($query, $bindings, \PDO::FETCH_COLUMN | \PDO::FETCH_UNIQUE, $column) && is_array($this->getResult())) {
                    return $this->getResult();
                }
            }
        } catch (\Throwable $e) {
            throw new \RuntimeException('Failed to select unique rows with `'.$e->getMessage().'`', 0, $e);
        }
        return [];
    }
    
    /**
     * Select random row(s) or value(s) from table
     * @param string $table  Table to select from
     * @param string $column Optional column, if you want to select only 1 column
     * @param int    $number Maximum number of rows to select
     *
     * @return array
     */
    public function selectRandom(string $table, string $column = '', int $number = 1): array
    {
        try {
            return $this->selectAll('SELECT '.(empty($column) ? '*' : '`'.$column.'`').' FROM `'.$table.'` ORDER BY RAND() LIMIT :number;', [':number' => [(max($number, 1)), 'int']]);
        } catch (\Throwable $e) {
            throw new \RuntimeException('Failed to select random rows with `'.$e->getMessage().'`', 0, $e);
        }
    }
    
    /**
     * Returns count value from SELECT.
     * @param string $query    `SELECT COUNT()` query. It can have other columns, but they will be ignored.
     * @param array  $bindings List of bindings.
     *
     * @return int
     */
    public function count(string $query, array $bindings = []): int
    {
        if (preg_match('/^\s*SELECT COUNT/mi', $query) === 1) {
            self::$queries++;
            try {
                if ($this->query($query, $bindings, \PDO::FETCH_COLUMN, 0) && is_array($this->getResult())) {
                    if (empty($this->getResult())) {
                        return 0;
                    }
                    return (int)$this->getResult()[0];
                }
                return 0;
            } catch (\Throwable $e) {
                throw new \RuntimeException('Failed to count rows with `'.$e->getMessage().'`', 0, $e);
            }
        } else {
            throw new \UnexpectedValueException('Query is not SELECT COUNT.');
        }
    }
    
    /**
     * Returns boolean value indicating, if anything matching SELECT exists.
     * @param string $query     Query to run
     * @param array  $bindings  List of bindings
     * @param int    $fetchMode Fetch mode
     *
     * @return bool
     */
    public function check(string $query, array $bindings = [], int $fetchMode = \PDO::FETCH_ASSOC): bool
    {
        try {
            if ($this->isSelect($query)) {
                self::$queries++;
                if ($this->query($query, $bindings, $fetchMode) && is_array($this->getResult()) && !empty($this->getResult())) {
                    return true;
                }
            }
        } catch (\Throwable $e) {
            throw new \RuntimeException('Failed to check if value exists with `'.$e->getMessage().'`', 0, $e);
        }
        return false;
    }
    
    /**
     * Check if table exists
     * @param string $table  Table name
     * @param string $schema Optional (but recommended) schema name
     *
     * @return bool
     */
    public function checkTable(string $table, string $schema = ''): bool
    {
        try {
            #Adjust query depending on whether schema is set
            if (empty($schema)) {
                $query = 'SELECT `TABLE_NAME` FROM `information_schema`.`TABLES` WHERE `TABLE_NAME` = :table;';
                $bindings = [':table' => $table];
            } else {
                $query = 'SELECT `TABLE_NAME` FROM `information_schema`.`TABLES` WHERE `TABLE_NAME` = :table AND `TABLE_SCHEMA` = :schema;';
                $bindings = [':table' => $table, ':schema' => $schema];
            }
            return $this->check($query, $bindings);
        } catch (\Throwable $e) {
            throw new \RuntimeException('Failed to check if table exists with `'.$e->getMessage().'`', 0, $e);
        }
    }
    
    /**
     * Check if a column exists in a table
     * @param string $table  Table name
     * @param string $column Column to check
     * @param string $schema Optional (but recommended) schema name
     *
     * @return bool
     */
    public function checkColumn(string $table, string $column, string $schema = ''): bool
    {
        try {
            #Adjust query depending on whether schema is set
            if (empty($schema)) {
                $query = 'SELECT `COLUMN_NAME` FROM `information_schema`.`COLUMNS` WHERE `TABLE_NAME` = :table AND `COLUMN_NAME` = :column;';
                $bindings = [':table' => $table, ':column' => $column];
            } else {
                $query = 'SELECT `COLUMN_NAME` FROM `information_schema`.`COLUMNS` WHERE `TABLE_NAME` = :table AND `COLUMN_NAME` = :column AND `TABLE_SCHEMA` = :schema;';
                $bindings = [':table' => $table, ':column' => $column, ':schema' => $schema];
            }
            return $this->check($query, $bindings);
        } catch (\Throwable $e) {
            throw new \RuntimeException('Failed to check if column exists with `'.$e->getMessage().'`', 0, $e);
        }
    }
    
    /**
     * Returns array of counts  for each unique value in column. Does not use bindings, so be careful not to process user input directly.
     * @param string $table        Table name to count in
     * @param string $columnName   Column to count
     * @param string $where        Optional WHERE condition. Full notations (`table`.`column`) are advised.
     * @param string $joinTable    Optional table to JOIN with
     * @param string $joinType     Type of JOIN to use (`INNER` by default)
     * @param string $joinOn       Column to JOIN on (defaults to the same column name we are counting)
     * @param string $joinReturn   Mandatory in case we use JOIN. If it's not set we do not know what to GROUP by but the original column, doing which with a JOIN will make no sense, since JOIN will be useful only to, for example, replace IDs with respective names. Full notations (`table`.`column`) are advised.
     * @param string $order        DESC (default) or ASC order the output by `count`
     * @param int    $limit        Optional limit of the output
     * @param array  $extraGroup   Optional list (array) of column names to GROUP by BEFORE the original $columnName or $joinReturn. Full notations (`table`.`column`) are advised.
     * @param bool   $altJoin      Apply JOIN logic AFTER the original COUNT. In some cases this may provide significant performance improvement, since we will be JOINing only the result, not the whole table. This approach is disabled by default, because depending on what is sent in $joinReturn and $extraGroup it may easily fail or provide unexpected results.
     * @param array  $extraColumns Optional list of additional columns to return on initial SELECT. May sometimes help with errors in case of $altJoin. If this is used you can use `tempresult` in $joinReturn.
     *
     * @return array
     */
    public function countUnique(string $table, string $columnName, string $where = '', string $joinTable = '', string $joinType = 'INNER', string $joinOn = '', string $joinReturn = '', string $order = 'DESC', int $limit = 0, array $extraGroup = [], bool $altJoin = false, array $extraColumns = []): array
    {
        #Prevent negative LIMIT
        if ($limit < 0) {
            $limit = 0;
        }
        #Sanitize ORDER
        if (preg_match('/(DESC|ASC)/mi', $order) !== 1) {
            $order = 'DESC';
        }
        #Building query
        if ($joinTable === '') {
            $query = 'SELECT '.(empty($extraColumns) ? '' : implode(', ', $extraColumns).', ').'`'.$table.'`.`'.$columnName.'` AS `value`, count(`'.$table.'`.`'.$columnName.'`) AS `count` FROM `'.$table.'` '.($where === '' ? '' : 'WHERE '.$where.' ').'GROUP BY '.(empty($extraGroup) ? '' : implode(', ', $extraGroup).', ').'`value` ORDER BY `count` '.$order.($limit === 0 ? '' : ' LIMIT '.$limit);
        } elseif (preg_match('/(NATURAL )?((INNER|CROSS)|((LEFT|RIGHT)$)|(((LEFT|RIGHT)\s*)?OUTER))/mi', $joinType) === 1) {
            #Check if we have a setup to return after JOIN
            if (empty($joinReturn)) {
                throw new \UnexpectedValueException('No value to return after JOIN was provided.');
            }
            #Check of we have a column to join on. If not - set its name to the name of original column
            if (empty($joinOn)) {
                $joinOn = $columnName;
            }
            if ($altJoin) {
                $query = 'SELECT '.$joinReturn.' AS `value`, `count` FROM (SELECT '.(empty($extraColumns) ? '' : implode(', ', $extraColumns).', ').'`'.$table.'`.`'.$columnName.'`, count(`'.$table.'`.`'.$columnName.'`) AS `count` FROM `'.$table.'` '.($where === '' ? '' : 'WHERE '.$where.' ').'GROUP BY '.(empty($extraGroup) ? '' : implode(', ', $extraGroup).', ').'`'.$table.'`.`'.$columnName.'` ORDER BY `count` '.$order.($limit === 0 ? '' : ' LIMIT '.$limit).') `tempresult` INNER JOIN `'.$joinTable.'` ON `tempresult`.`'.$columnName.'`=`'.$joinTable.'`.`'.$joinOn.'` ORDER BY `count` '.$order;
            } else {
                $query = 'SELECT '.$joinReturn.' AS `value`, count(`'.$table.'`.`'.$columnName.'`) AS `count` FROM `'.$table.'` INNER JOIN `'.$joinTable.'` ON `'.$table.'`.`'.$columnName.'`=`'.$joinTable.'`.`'.$joinOn.'` '.($where === '' ? '' : 'WHERE '.$where.' ').'GROUP BY '.(empty($extraGroup) ? '' : implode(', ', $extraGroup).', ').'`value` ORDER BY `count` '.$order.($limit === 0 ? '' : ' LIMIT '.$limit);
            }
        } else {
            throw new \UnexpectedValueException('Unsupported type of JOIN ('.$joinType.') was provided.');
        }
        self::$queries++;
        try {
            if ($this->query($query) && is_array($this->getResult())) {
                return $this->getResult();
            }
            return [];
        } catch (\Throwable $e) {
            throw new \RuntimeException('Failed to count unique values with `'.$e->getMessage().'`', 0, $e);
        }
    }
    
    /**
     * Similar to countUnique, but utilizes SUM based on comparison of the column's values against the list provided. Each `value` will be present in a separate column. In some cases your results will look like transposed countUnique, but in other cases this can provide some more flexibility in terms of how to structure them.
     * @param string $table      Table name to count in
     * @param string $columnName Column to count
     * @param array  $values     List of values to check for. Defaults to 0 and 1 (boolean)
     * @param array  $names      List of names for resulting columns. Defaults to `false` and `true`
     * @param string $where      Optional WHERE condition. Full notations (`table`.`column`) are advised.
     * @param string $joinTable  Optional table to JOIN with
     * @param string $joinType   Type of JOIN to use (`INNER` by default)
     * @param string $joinOn     Column to JOIN on (defaults to the same column name we are counting).
     * @param string $joinReturn Mandatory in case we use JOIN. If it's not set we do not know what to GROUP by but the original column, doing which with a JOIN will make no sense, since JOIN will be useful only to, for example, replace IDs with respective names. Full notations (`table`.`column`) are advised.
     * @param string $order      DESC (default) or ASC order the output by 1st column
     * @param int    $limit      Optional limit of the output
     * @param array  $extraGroup Optional list (array) of column names to GROUP by BEFORE the original $columnName or $joinReturn. Full notations (`table`.`column`) are advised.
     *
     * @return array
     */
    public function sumUnique(string $table, string $columnName, array $values = [], array $names = [], string $where = '', string $joinTable = '', string $joinType = 'INNER', string $joinOn = '', string $joinReturn = '', string $order = 'DESC', int $limit = 0, array $extraGroup = []): array
    {
        #Default $values
        if (empty($values) === true) {
            $values = [0, 1];
        } else {
            #Ensure we use regular array, not associative one
            $values = array_values($values);
        }
        #Default $names
        if (empty($names) === true) {
            $names = ['false', 'true'];
        } else {
            #Ensure we use regular array, not associative one
            $names = array_values($names);
        }
        #Check that both $values and $names have identical length
        if (count($values) !== count($names)) {
            throw new \UnexpectedValueException('Array of names provided to sumUnique function has different number of elements than array of values ('.count($names).' instead of '.count($values).')');
        }
        #Prevent negative LIMIT
        if ($limit < 0) {
            $limit = 0;
        }
        #Sanitize ORDER
        if (preg_match('/(DESC|ASC)/mi', $order) !== 1) {
            $order = 'DESC';
        }
        #Build conditional fields
        $sumFields = [];
        $bindings = [];
        foreach ($values as $key => $value) {
            $sumFields[] = 'SUM(IF(`'.$table.'`.`'.$columnName.'` = :'.$key.', 1, 0)) AS `'.$names[$key].'`';
            $bindings[':'.$key] = (string)$value;
        }
        #Building query
        if ($joinTable === '') {
            $query = 'SELECT '.implode(', ', $sumFields).' FROM `'.$table.'` '.($where === '' ? '' : 'WHERE '.$where.' ').'GROUP BY '.(empty($extraGroup) ? '' : implode(', ', $extraGroup).', ').'1 ORDER BY 1 '.$order.($limit === 0 ? '' : ' LIMIT '.$limit);
        } elseif (preg_match('/(NATURAL )?((INNER|CROSS)|((LEFT|RIGHT)$)|(((LEFT|RIGHT)\s*)?OUTER))/mi', $joinType) === 1) {
            #Check if we have a setup to return after JOIN
            if (empty($joinReturn)) {
                throw new \UnexpectedValueException('No value to return after JOIN was provided.');
            }
            #Check of we have a column to join on. If not - set its name to the name of original column
            if (empty($joinOn)) {
                $joinOn = $columnName;
            }
            $query = 'SELECT '.$joinReturn.', '.implode(', ', $sumFields).' FROM `'.$table.'` INNER JOIN `'.$joinTable.'` ON `'.$table.'`.`'.$columnName.'`=`'.$joinTable.'`.`'.$joinOn.'` '.($where === '' ? '' : 'WHERE '.$where.' ').'GROUP BY '.(empty($extraGroup) ? '' : implode(', ', $extraGroup).', ').'1 ORDER BY 1 '.$order.($limit === 0 ? '' : ' LIMIT '.$limit);
        } else {
            throw new \UnexpectedValueException('Unsupported type of JOIN ('.$joinType.') was provided.');
        }
        self::$queries++;
        try {
            if ($this->query($query, $bindings) && is_array($this->getResult())) {
                return $this->getResult();
            }
            return [];
        } catch (\Throwable $e) {
            throw new \RuntimeException('Failed to sum unique values with `'.$e->getMessage().'`', 0, $e);
        }
    }
    
    /**
     * Function useful for inserting into tables with AUTO INCREMENT. If INSERT is successful, and lastId is supported, will return the ID inserted, otherwise will return false
     * @param string $query    `INSERT` query to process
     * @param array  $bindings List of bindings
     *
     * @return string|false
     */
    public function insertAI(string $query, array $bindings = []): string|false
    {
        #Check if query is insert
        if (preg_match('/^\s*INSERT\s+INTO/ui', $query) !== 1) {
            throw new \UnexpectedValueException('Query is not INSERT.');
        }
        #Check that we have only 1 query
        $queries = $this->stringToQueries($query);
        if (count($queries) > 1) {
            throw new \UnexpectedValueException('String provided seems to contain multiple queries.');
        }
        self::$queries++;
        if ($this->query($query, $bindings)) {
            return $this->lastId;
        }
        return false;
    }
    
    /**
     * Function to get list of all tables for schema in order, where first you have tables without dependencies (no foreign keys), and then tables that are dependent on tables that has come before. This is useful if you want to dump backups in specific order, so that you can then restore the data without disabling foreign keys.
     * Only for MySQL/MariaDB
     *
     * @param string $schema Optional name of the schema to limit to
     *
     * @return array
     */
    public function showOrderedTables(string $schema = ''): array
    {
        #This is the list of tables, that we will return in the end
        $tablesOrderedFull = [];
        #This is the list of the same tables, but where every element is a string of format `schema`.`table`. Used for array search purposes only
        $tablesNamesOnly = [];
        #Get all tables except standard system ones and also order them by size
        $tablesRaw = $this->selectAll('SELECT `TABLE_SCHEMA` as `schema`, `TABLE_NAME` as `table` FROM INFORMATION_SCHEMA.TABLES WHERE `TABLE_SCHEMA` NOT IN (\'information_schema\', \'performance_schema\', \'mysql\', \'sys\', \'test\')'.(empty($schema) ? '' : ' AND `TABLE_SCHEMA`=:schema').' ORDER BY (DATA_LENGTH+INDEX_LENGTH);', (empty($schema) ? [] : [':schema' => [$schema, 'string']]));
        #Get dependencies
        foreach ($tablesRaw as $key => $table) {
            $table['dependencies'] = $this->selectAllDependencies($table['schema'], $table['table']);
            if (count($table['dependencies']) === 0) {
                #Add this to ordered list right away, if we have no dependencies
                $tablesOrderedFull[] = $table;
                $tablesNamesOnly[] = '`'.$table['schema'].'`.`'.$table['table'].'`';
                unset($tablesRaw[$key]);
            } else {
                #Update raw list with dependencies to use further
                $tablesRaw[$key] = $table;
            }
        }
        #Check if we have any cyclic references among the remaining tables
        if ($this->checkCyclicForeignKeys($tablesRaw)) {
            #Throw an error, because with cyclic references there is no way to determine the order at all
            throw new \PDOException('Cyclic foreign key references detected.');
        }
        while (count($tablesRaw) > 0) {
            foreach ($tablesRaw as $key => $table) {
                #Check if table is all tables from dependencies list is already present in the ordered list
                foreach ($table['dependencies'] as $dKey => $dependency) {
                    #If a dependency is not already present in the list of tables - go to next table
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
        return $tablesOrderedFull;
    }
    
    /**
     * This function allows you to check for cyclic foreign keys, when 2 (or more) tables depend on each other.
     * This is considered bad practice even with nullable columns, but you may easily miss them as your database grows, especially if you have chains of 3 or more tables.
     * This will not return the specific FKs you need to deal with, but rather just list of tables referencing tables, that refer the initial ones.
     * You will need to analyze the references yourself in order to "untangle" them properly.
     * You can pass prepared list of tables with format of ['schema' => 'schema_name', 'table' => 'table_name'].
     * This array can also include key 'dependencies' which should be an array of values like '`schema_name`.`table_name`',
     * Only for MySQL/MariaDB
     *
     * @param array|null $tables Optional list of tables in `schema.table` format. If none is provided, will first get list of all tables available.
     *
     * @return bool
     */
    public function checkCyclicForeignKeys(?array $tables = null): bool
    {
        #Unfortunately I was not able to make things work with just 1 query with a recursive sub-query, so doing things in 2 steps.
        #First step is to get all tables that have FKs, but exclude those that refer themselves
        if ($tables === null) {
            $tables = $this->selectAll('SELECT `TABLE_SCHEMA` AS `schema`, `TABLE_NAME` AS `table` FROM `information_schema`.`KEY_COLUMN_USAGE` WHERE `REFERENCED_TABLE_SCHEMA` IS NOT NULL AND CONCAT(`REFERENCED_TABLE_SCHEMA`, \'.\', `REFERENCED_TABLE_NAME`) != CONCAT(`TABLE_SCHEMA`, \'.\', `TABLE_NAME`) GROUP BY `TABLE_SCHEMA`, `TABLE_NAME`;');
        }
        foreach ($tables as $key => $table) {
            #For each table get their recursive list of dependencies, if not set in the prepared array
            if (!isset($table['dependencies'])) {
                $table['dependencies'] = $this->selectAllDependencies($table['schema'], $table['table']);
            }
            #Check if dependencies list has the table itself
            if (in_array('`'.$table['schema'].'`.`'.$table['table'].'`', $table['dependencies'], true)) {
                #Update the list (only really needed if we did not have prepared list of tables from the start)
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
    public function selectAllDependencies(string $schema, string $table): array
    {
        #We are using backticks when comparing the schemas and tables, since that will definitely avoid any matches due to dots in names
        return $this->selectColumn('
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
     * Due to SHOW CREATE TABLE being special, we can't use it as sub-query, so need to do 2 queries instead.
     * Only for MySQL/MariaDB
     *
     * @param string $schema Schema name
     * @param string $table  Table name
     *
     * @return string|null
     */
    public function showCreateTable(string $schema, string $table): ?string
    {
        #Get the original create function
        $create = $this->selectValue('SHOW CREATE TABLE `'.$schema.'`.`'.$table.'`;', [], 1);
        #Add semicolon for consistency
        if (!str_ends_with(';', $create)) {
            $create .= ';';
        }
        #Get current ROW_FORMAT value
        $rowFormat = $this->selectValue('SELECT `ROW_FORMAT` FROM `information_schema`.`TABLES` WHERE `TABLE_SCHEMA`=:schema AND `TABLE_NAME`=:table;', [':schema' => $schema, ':table' => $table]);
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
        #Return result
        return $create;
    }
    
    /**
     * Helper function to allow splitting a string into array of queries. Made public, because it may be useful outside this class' functions.
     * Regexp taken from https://stackoverflow.com/questions/24423260/split-sql-statements-in-php-on-semicolons-but-not-inside-quotes
     *
     * @param string $string
     *
     * @return array
     */
    public function stringToQueries(string $string): array
    {
        return preg_split('~\([^)]*\)(*SKIP)(*FAIL)|(?<=;)(?! *$)~', $string);
    }
    
    /**
     * Helper function to check if query is a select(able) one
     * @param string $query Query to check
     * @param bool   $throw Throw exception if not `SELECT` and this option is `true`.
     *
     * @return bool
     */
    private function isSelect(string $query, bool $throw = true): bool
    {
        if (preg_match('/^\s*(\(\s*)*('.implode('|', self::selects).')/mi', $query) === 1) {
            return true;
        }
        if ($throw) {
            throw new \UnexpectedValueException('Query is not one of '.implode(', ', self::selects).'.');
        }
        return false;
    }
    
    /**
     * Return result. Used to prevent modification of the results from outside.
     * @return mixed|null
     */
    public function getResult(): mixed
    {
        return $this->result;
    }
}
