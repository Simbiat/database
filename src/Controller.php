<?php
declare(strict_types=1);
namespace SimbiatDB;

class Controller
{    
    #List of functions, that may return rows
    const selects = [
        'SELECT', 'SHOW', 'HANDLER', 'ANALYZE', 'CHECK', 'DESCRIBE', 'DESC', 'EXPLAIN', 'HELP'
    ];
    #Static for convinience, in case object gets destroyed, but you still want to get total number
    public static $queries = 0;
    private object $dbh;
    private bool $debug = false;
    private int $maxruntime = 3600; #in seconds
    private int $maxtries = 5;
    private int $sleep = 5; #in seconds
    private $result = NULL;
    
    public function __construct()
    {
        $this->dbh = (new \SimbiatDB\Pool)->openConnection();
    }
    
    public function query($queries, array $bindings = [], $fetch_style = \PDO::FETCH_ASSOC, $fetch_argument = NULL, array $ctor_args = []): bool
    {
        $try = 0;
        do {
            try {
                $try++;
                if (is_string($queries)) {
                    $sql = $this->dbh->prepare($queries);
                    #Preparing bindings
                    $sql = $this->binding($sql, $bindings);
                    set_time_limit($this->maxruntime);
                    if ($this->debug) {
                        echo $queries.'<br>';
                        ob_flush();
                        flush();
                    }
                    $sql->execute();
                    self::$queries++;
                    if ($this->debug) {
                        $sql->debugDumpParams();
                        ob_flush();
                        flush();
                    }
                    if (preg_match('/^\s*\(*'.implode('|', self::selects).'/mi', $queries) === 1) {
                        if ($fetch_argument === 'row') {
                            $this->result = $sql->fetch($fetch_style);
                        } elseif ($fetch_style === \PDO::FETCH_COLUMN || $fetch_style === \PDO::FETCH_FUNC) {
                            $this->result = $sql->fetchAll($fetch_style, $fetch_argument);
                        } elseif ($fetch_style === \PDO::FETCH_CLASS) {
                            $this->result = $sql->fetchAll($fetch_style, $fetch_argument, $ctor_args);
                        } else {
                            $this->result = $sql->fetchAll($fetch_style);
                        }
                    } else {
                        $this->result = $sql->rowCount();
                    }
                    return true;
                } else {
                    if (!is_array($queries)) {
                        throw new \UnexpectedValueException('Queries sent are neither string nor array.');
                    }
                    $this->dbh->beginTransaction();
                    foreach ($queries as $sequence=>$query) {
                        if (is_string($query)) {
                            $actualquery = $query;
                        } else {
                            if (is_array($query)) {
                                if (!is_string($query[0])) {
                                    throw new \UnexpectedValueException('Query #'.$sequence.' in bulk is not a string.');
                                } else {
                                    $actualquery = $query[0];
                                }
                            } else {
                                if (!is_string($query)) {
                                    throw new \UnexpectedValueException('Query #'.$sequence.' in bulk is not a string.');
                                }
                            }
                        }
                        #Check if it's a command which may return rows
                        if (preg_match('/(^('.implode('|', self::selects).'))/i', $actualquery) === 1 && preg_match('/^SELECT.*FOR UPDATE$/mi', $actualquery) !== 1) {
                            trigger_error('A selector command ('.implode(', ', self::selects).') detected in bulk of queries. Output wll not be fetched and may result in errors in further queries. Consider revising: '.$actualquery);
                        }
                        $sql = $this->dbh->prepare($actualquery);
                        #Preparing bindings
                        if (is_array($query)) {
                            if (!empty($query[1])) {
                                if (is_array($query[1])) {
                                    $sql = $this->binding($sql, array_merge($bindings, $query[1]));
                                } else {
                                    throw new \UnexpectedValueException('Bindings provided for query #'.$sequence.' are not an array.');
                                }
                            }
                        }
                        #Increasing time limit for potentially long operations (like optimize)
                        set_time_limit($this->maxruntime);
                        if ($this->debug) {
                            echo $actualquery.'<br>';
                            ob_flush();
                            flush();
                        }
                        $sql->execute();
                        self::$queries++;
                        if ($this->debug) {
                            $sql->debugDumpParams();
                            ob_flush();
                            flush();
                        }
                    }
                    $this->dbh->commit();
                    return true;
                }
            } catch (\Exception $e) {
                $error = $e->getMessage().$e->getTraceAsString();
                if (isset($sql) && $this->debug) {
                    $sql->debugDumpParams();
                    echo $error;
                    ob_flush();
                    flush();
                }
                #Check if deadlock
                if (isset($sql) && ($sql->errorCode() == '40001' || (is_string($sql->errorInfo()[2]) && preg_match('.*deadlock.*/mis', $sql->errorInfo()[2]) === 1 ))) {
                    $deadlock = true;
                    if ($try == $this->maxtries) {
                        error_log($error);
                    }
                } else {
                    $deadlock = false;
                    error_log($error);
                }
                if ($this->dbh->inTransaction()) {
                    $this->dbh->rollBack();
                    if (!$deadlock) {
                        throw $e;
                    }
                }
                #If deadlock - sleep and then retry
                if ($deadlock) {
                    sleep($this->sleep);
                    continue;
                } else {
                    throw $e;
                }
            }
            break;
        } while ($try <= $this->maxtries);
        throw new \Exception('Deadlock encountered for set maximum of '.$this->maxtries.' tries.');
    }
    
    #Function mainly for convinience and some types enforcing, which sometimes 'fail' in PDO itself
    private function binding(\PDOStatement $sql, array $bindings = []): \PDOStatement
    {
        foreach ($bindings as $binding=>$value) {
            if (!is_array($value)) {
                $sql->bindValue($binding, $value);
            } else {
                switch(strtolower($value[1])) {
                    case 'date':
                        $sql->bindValue($binding, $this->time($value[0], 'Y-m-d'), \PDO::PARAM_STR);
                        break;
                    case 'time':
                        $sql->bindValue($binding, $this->time($value[0], 'Y-m-d H:i:s.u'), \PDO::PARAM_STR);
                        break;
                    case 'bool':
                    case 'boolean':
                        $sql->bindValue($binding, boolval($value[0]), \PDO::PARAM_BOOL);
                        break;
                    case 'null':
                        $sql->bindValue($binding, NULL, \PDO::PARAM_NULL);
                        break;
                    case 'int':
                    case 'integer':
                    case 'number':
                    case 'limit':
                    case 'offset':
                        $sql->bindValue($binding, intval($value[0]), \PDO::PARAM_INT);
                        break;
                    case 'str':
                    case 'string':
                    case 'text':
                    case 'float':
                    case 'varchar':
                    case 'varchar2':
                        $sql->bindValue($binding, strval($value[0]), \PDO::PARAM_STR);
                        break;
                    case 'lob':
                    case 'large':
                    case 'object':
                    case 'blob':
                        $sql->bindParam($binding, $value[0], \PDO::PARAM_LOB, strlen($value[0]));
                        break;
                    case 'like':
                        $sql->bindValue($binding, '%'.$value[0].'%', \PDO::PARAM_STR);
                        break;
                    default:
                        if (is_int($value[1])) {
                            $sql->bindValue($binding, $value[0], $value[1]);
                        } else {
                            $sql->bindValue($binding, strval($value[0]), \PDO::PARAM_STR);
                        }
                }
            }
        }
        return $sql;
    }
    
    private function time($time = 0, string $format = 'Y-m-d H:i:s.u'): string
    {
        return (new \SandClock\Api)->setFormat($format)->format($time);
    }
    
    ##########################
    #Useful semantic wrappers#
    ##########################
    #Return full results as multidimensional array (associative by default).
    public function selectAll(string $query, array $bindings = [], $fetchmode = \PDO::FETCH_ASSOC): array
    {
        if (preg_match('/^\s*\(*'.implode('|', self::selects).'/mi', $query) === 1) {
            if ($this->query($query, $bindings, $fetchmode) && is_array($this->getResult())) {
                return $this->getResult();
            } else {
                return [];
            }
        } else {
            throw new \UnexpectedValueException('Query is not one of '.implode(', ', self::selects).'.');
        }
    }
    
    #Returns only 1 row from SELECT (essentially LIMIT 1).
    public function selectRow(string $query, array $bindings = [], $fetchmode = \PDO::FETCH_ASSOC): array
    {
        if (preg_match('/^\s*\(*'.implode('|', self::selects).'/mi', $query) === 1) {
            if ($this->query($query, $bindings, $fetchmode, 'row') && is_array($this->getResult())) {
                return $this->getResult();
            } else {
                return [];
            }
        } else {
            throw new \UnexpectedValueException('Query is not one of '.implode(', ', self::selects).'.');
        }
    }
    
    #Returns column (frist by default) even if original SELECT requests for more. Change 3rd parameter accordingly to use another column as key (starting from 0).
    public function selectColumn(string $query, array $bindings = [], int $column = 0): array
    {
        if (preg_match('/^\s*\(*'.implode('|', self::selects).'/mi', $query) === 1) {
            if ($this->query($query, $bindings, \PDO::FETCH_COLUMN, $column) && is_array($this->getResult())) {
                return $this->getResult();
            } else {
                return [];
            }
        } else {
            throw new \UnexpectedValueException('Query is not one of '.implode(', ', self::selects).'.');
        }
    }
    
    #Returns a value directly, instead of array containing that value. Useful for getting specific settings from DB. No return typing, since it may vary, so be careful with that.
    public function selectValue(string $query, array $bindings = [], int $column = 0)
    {
        if (preg_match('/^\s*\(*'.implode('|', self::selects).'/mi', $query) === 1) {
            if ($this->query($query, $bindings, \PDO::FETCH_COLUMN, $column) && is_array($this->getResult())) {
                return $this->getResult()[$column];
            } else {
                return [];
            }
        } else {
            throw new \UnexpectedValueException('Query is not one of '.implode(', ', self::selects).'.');
        }
    }
    
    #Returns key->value pair(s) based on 2 columns. First column (by default) is used as key. Change 3rd parameter accordingly to use another column as key (starting from 0).
    public function selectPair(string $query, array $bindings = [], int $column = 0): array
    {
        if (preg_match('/^\s*\(*'.implode('|', self::selects).'/mi', $query) === 1) {
            if ($this->query($query, $bindings, \PDO::FETCH_KEY_PAIR, $column) && is_array($this->getResult())) {
                return $this->getResult();
            } else {
                return [];
            }
        } else {
            throw new \UnexpectedValueException('Query is not one of '.implode(', ', self::selects).'.');
        }
    }
    
    #Returns unique values from a column (first by default). Change 3rd parameter accordingly to use another column as key (starting from 0).
    public function selectUnique(string $query, array $bindings = [], int $column = 0): array
    {
        if (preg_match('/^\s*\(*'.implode('|', self::selects).'/mi', $query) === 1) {
            if ($this->query($query, $bindings, \PDO::FETCH_COLUMN|\PDO::FETCH_UNIQUE, $column) && is_array($this->getResult())) {
                return $this->getResult();
            } else {
                return [];
            }
        } else {
            throw new \UnexpectedValueException('Query is not one of '.implode(', ', self::selects).'.');
        }
    }
    
    #Returns count value from SELECT.
    public function count(string $query, array $bindings = array()): int
    {
        if (preg_match('/^\s*SELECT COUNT/mi', $query) === 1) {
            if ($this->query($query, $bindings, \PDO::FETCH_COLUMN, 0) && is_array($this->getResult())) {
                if (empty($this->getResult())) {
                    return 0;
                } else {
                    return intval($this->getResult()[0]);
                }
            } else {
                return 0;
            }
        } else {
            throw new \UnexpectedValueException('Query is not one of '.implode(', ', self::selects).' COUNT.');
        }
    }
    
    #Returns boolean value indicating, if anything matching SELECT exists.
    public function check(string $query, array $bindings = [], $fetchmode = \PDO::FETCH_ASSOC): bool
    {
        if (preg_match('/^\s*\(*'.implode('|', self::selects).'/mi', $query) === 1) {
            if ($this->query($query, $bindings, $fetchmode) && is_array($this->getResult()) && !empty($this->getResult())) {
                return true;
            } else {
                return false;
            }
        } else {
            throw new \UnexpectedValueException('Query is not one of '.implode(', ', self::selects).'.');
        }
    }
    
    #####################
    #Setters and getters#
    #####################
    public function getMaxTime(): int
    {
        return $this->maxruntime;
    }
    
    public function setMaxTime(int $seconds): self
    {
        $this->maxruntime = $seconds;
        return $this;
    }
    
    public function getDebug(): bool
    {
        return $this->debug;
    }
    
    public function setDebug(bool $debug): self
    {
        $this->debug = $debug;
        return $this;
    }
    
    public function getTries(): int
    {
        return $this->maxtries;
    }
    
    public function setTries(int $tries): self
    {
        $this->maxtries = abs($tries);
        return $this;
    }
    
    public function getSleep(): int
    {
        return $this->sleep;
    }
    
    public function setSleep(int $sleep): self
    {
        $this->sleep = abs($sleep);
        return $this;
    }
    
    public function getResult()
    {
        return $this->result;
    }
    
    #Simply for convinience, in case you don't want to call a static
    public function getQueries(): int
    {
        return self::$queries;
    }
}
?>
