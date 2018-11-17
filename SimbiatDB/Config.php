<?php
declare(strict_types=1);
namespace SimbiatDB;

final class Config
{
    private $user = NULL;
    private $password = NULL;
    private $driver = 'mysql';
    private $host = 'localhost';
    private $port = NULL;
    private $socket = NULL;
    private $dbname = NULL;
    private $charset = 'utf8mb4';
    private $PDOptions = [
        \PDO::ATTR_ERRMODE => \PDO::ERRMODE_EXCEPTION,
        \PDO::ATTR_PERSISTENT => false,
        \PDO::ATTR_ORACLE_NULLS => \PDO::NULL_NATURAL,
        \PDO::ATTR_DEFAULT_FETCH_MODE => \PDO::FETCH_ASSOC,
        \PDO::ATTR_EMULATE_PREPARES => true,
    ];
    
    public function setUser(string $user): self
    {
        if (empty($user)) {
            throw new \InvalidArgumentException('Attempted to set empty user.');
        } else {
            $this->user=$user;
        }
        return $this;
    }
    
    public function getUser(): string
    {
        return (empty($this->user) ? '' : $this->user);
    }
    
    public function setPassword(string $password = ''): self
    {
        $this->password=$password;
        return $this;
    }
    
    public function getPassword(): string
    {
        #Restricting direct access to password for additional security
        $caller = debug_backtrace();
        if (empty($caller[1])) {
            throw new \RuntimeException('Direct call detected. Access denied.');
        } else {
            $caller = $caller[1];
        }
        if ($caller['function'] !== 'openConnection' || $caller['class'] !== 'SimbiatDB\\Pool') {
            throw new \RuntimeException('Call from non-allowed function or object-type detected. Access denied.');
        }
        return (empty($this->password) ? '' : $this->password);
    }
    
    public function setHost(string $host = 'localhost', int $port = NULL, string $socket = NULL): self
    {
        $this->host=(empty($host) ? 'localhost' : $host);
        $this->port=($port<1 ? NULL : ($port>65535 ? NULL : $port));
        $this->socket=$socket;
        return $this;
    }
    
    public function getHost(): string
    {
        if (empty($this->socket)) {
            return 'host='.$this->host.';'.(empty($this->port) ? '' : 'port='.$this->port.';');
        } else {
            return 'unix_socket='.$this->socket.';';
        }
    }
    
    public function setDriver(string $driver = 'mysql'): self
    {
        if (in_array($driver, \PDO::getAvailableDrivers())) {
            $this->driver=$driver;
        } else {
            throw new \InvalidArgumentException('Attempted to set unsupported driver.');
        }
        return $this;
    }
    
    public function getDriver(): string
    {
        return (empty($this->driver) ? '' : $this->driver);
    }
    
    public function setDB(string $dbname): self
    {
        if (empty($dbname)) {
            throw new \InvalidArgumentException('Attempted to set empty database name.');
        } else {
            $this->dbname=$dbname;
        }
        return $this;
    }
    
    public function getDB(): string
    {
        return (empty($this->dbname) ? '' : 'dbname='.$this->dbname.';');
    }
    
    public function setCharset(string $charset): self
    {
        $this->charset=(empty($charset) ? 'utf8mb4' : $charset);
        return $this;
    }
    
    public function getCharset(): string
    {
        return (empty($this->charset) ? '' : 'charset='.$this->charset.';');
    }
    
    public function getDSN(): string
    {
        if (empty($this->getDB())) {
            throw new \UnexpectedValueException('No database name is set.');
        } else {
            return $this->getDriver().':'.$this->getHost().$this->getDB().$this->getCharset();
        }
    }
    
    public function setOption(int $option, $value): self
    {
        if (
            in_array($option, [\PDO::ATTR_ERRMODE,\PDO::ATTR_EMULATE_PREPARES])
            ||
            ($this->getDriver() === 'mysql' && in_array($option, [\PDO::MYSQL_ATTR_MULTI_STATEMENTS,\PDO::MYSQL_ATTR_DIRECT_QUERY,\PDO::MYSQL_ATTR_IGNORE_SPACE]))
            ||
            ($this->getDriver() === 'sqlsrv' && in_array($option, [\PDO::SQLSRV_ATTR_DIRECT_QUERY]))
        ) {
            throw new \InvalidArgumentException('Attempted to set restricted attribute.');
        } else {
            $this->PDOptions[$option] = $value;
        }
        return $this;
    }
    
    public function getOptions(): array
    {
        if ($this->getDriver() === 'mysql') {
            $this->PDOptions[\PDO::MYSQL_ATTR_MULTI_STATEMENTS] = false;
            $this->PDOptions[\PDO::MYSQL_ATTR_IGNORE_SPACE] = true;
            $this->PDOptions[\PDO::MYSQL_ATTR_DIRECT_QUERY] = false;
        } elseif ($this->getDriver() === 'sqlsrv') {
            $this->PDOptions[\PDO::SQLSRV_ATTR_DIRECT_QUERY] = false;
        }
        return $this->PDOptions;
    }
    
    #prevent properties from showing in var_dump and print_r for additional security
    public function __debugInfo(): array
    {
        return [];
    }
}
?>