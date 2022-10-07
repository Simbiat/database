<?php
declare(strict_types=1);
namespace Simbiat\Database;

final class Config
{
    private ?string $user = NULL;
    private ?string $password = NULL;
    private string $driver = 'mysql';
    private string $host = 'localhost';
    private ?int $port = NULL;
    private ?string $socket = NULL;
    private ?string $dbname = NULL;
    private string $charset = 'utf8mb4';
    private string $appName = 'PHP Generic DB-lib';
    private ?string $role = NULL;
    private int $dialect = 3;
    private string $sslmode = 'verify-full';
    private string $customString = '';
    private array $PDOptions = [
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
        if ($caller['function'] !== 'openConnection' || $caller['class'] !== 'Simbiat\\Database\\Pool') {
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
        $this->charset = $charset ?? 'utf8mb4';
        return $this;
    }

    public function getCharset(): string
    {
        return (empty($this->charset) ? '' : 'charset='.$this->charset.';');
    }
    
    #For DB-Lib only
    public function setAppName(string $appName): self
    {
        $this->appName = $appName ?? 'PHP Generic DB-lib';
        return $this;
    }
    
    #For DB-Lib only
    public function getAppName(): string
    {
        return (empty($this->appName) ? '' : 'appname='.$this->appName.';');
    }
    
    #For Firebird only
    public function setRole(string $role): self
    {
        $this->role = $role ?? null;
        return $this;
    }
    
    #For Firebird only
    public function getRole(): string
    {
        return (empty($this->role) ? '' : 'role='.$this->role.';');
    }
    
    #For Firebird only
    public function setDialect(int $dialect): self
    {
        if ($dialect !== 0 && $dialect !== 3) {
            $dialect = 3;
        }
        $this->dialect = $dialect;
        return $this;
    }
    
    #For Firebird only
    public function getDialect(): string
    {
        return 'dialect='.$this->dialect.';';
    }
    
    #For PostgresSQL only
    public function setSSLMode(string $sslmode): self
    {
        if (!in_array($sslmode, ['disable', 'allow', 'prefer', 'require', 'verify-ca', 'verify-full'])) {
            $sslmode = 'verify-full';
        }
        $this->sslmode = $sslmode;
        return $this;
    }
    
    #For PostgresSQL only
    public function getSSLMode(): string
    {
        return 'sslmode='.$this->sslmode.';';
    }
    
    public function setCustomString(string $customString): self
    {
        #Remove username and password values
        $customString = preg_replace('/(Password|Pass|PWD|UID|User ID|User|Username)=[^;]+;/miu', '', $customString);
        $this->customString = $customString;
        return $this;
    }
    
    public function getCustomString(): string
    {
        return $this->customString;
    }
    
    #For IBM only
    public function getIBM(): string
    {
        $dbname = $this->getDB();
        if (preg_match('/.+\.ini$/ui', $dbname)) {
            return $dbname;
        } else {
            return 'DRIVER={IBM DB2 ODBC DRIVER};DATABASE='.$dbname.';HOSTNAME='.$this->host.';'.(empty($this->port) ? '' : 'PORT='.$this->port.';').'PROTOCOL=TCPIP;';
        }
    }
    
    #For Informix only
    public function getInformix(): string
    {
        return 'host='.$this->host.';'.(empty($this->port) ? '' : 'service='.$this->port.';').'database='.$this->dbname.';protocol=onsoctcp;EnableScrollableCursors=1;';
    }
    
    #For SQLLite only
    public function getSQLLite(): string
    {
        $dbname = $this->getDB();
        #Check if we are using in-memory DB
        if ($dbname === ':memory:') {
            return $dbname;
        } else {
            #Check if it's a file that exists
            if (is_file($dbname)) {
                return $dbname;
            } else {
                #Assume temporary database
                return '';
            }
        }
    }
    
    #For ODBC only
    public function getODBC(): string
    {
        return $this->dbname ?? '';
    }
    
    #For MS SQL Server only
    public function getSQLServer() : string
    {
        return 'Server='.$this->host.(empty($this->port) ? '' : ','.$this->port).';Database='.$this->dbname;
    }

    public function getDSN(): string
    {
        $dsn = match($this->getDriver()) {
            'mysql' => 'mysql:'.$this->getHost().$this->getDB().$this->getCharset(),
            'cubrid' => 'cubrid:'.$this->getHost().$this->getDB(),
            'sybase' => 'sybase:'.$this->getHost().$this->getDB().$this->getCharset().$this->getAppName(),
            'mssql' => 'mssql:'.$this->getHost().$this->getDB().$this->getCharset().$this->getAppName(),
            'dblib' => 'dblib:'.$this->getHost().$this->getDB().$this->getCharset().$this->getAppName(),
            'firebird' => 'firebird:'.$this->getDB().$this->getCharset().$this->getRole().$this->getDialect(),
            'pgsql' => 'pgsql:'.$this->getHost().$this->getDB().$this->getSSLMode(),
            'oci' => 'oci:'.$this->getDB().$this->getCharset(),
            'ibm' => 'ibm:'.$this->getIBM(),
            'informix' => 'informix:'.$this->getInformix(),
            'sqlite' => 'sqlite:'.$this->getSQLLite(),
            'odbc' => 'odbc:'.$this->getODBC(),
            'sqlsrv' => 'sqlsrv:'.$this->getSQLServer(),
            default => null,
        };
        if ($dsn) {
            #Return DSN while adding any custom values
            return $dsn.$this->getCustomString();
        } else {
            throw new \UnexpectedValueException('Unsupported driver.');
        }
    }

    public function setOption(int $option, mixed $value): self
    {
        if (
            in_array($option, [\PDO::ATTR_ERRMODE,\PDO::ATTR_EMULATE_PREPARES])
            ||
            ($this->getDriver() === 'mysql' && in_array($option, [\PDO::MYSQL_ATTR_MULTI_STATEMENTS,\PDO::MYSQL_ATTR_DIRECT_QUERY,\PDO::MYSQL_ATTR_IGNORE_SPACE,\PDO::MYSQL_ATTR_USE_BUFFERED_QUERY]))
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
            $this->PDOptions[\PDO::MYSQL_ATTR_USE_BUFFERED_QUERY] = true;
        } elseif ($this->getDriver() === 'sqlsrv') {
            $this->PDOptions[\PDO::SQLSRV_ATTR_DIRECT_QUERY] = false;
        }
        $this->PDOptions[\PDO::ATTR_EMULATE_PREPARES] = true;
        $this->PDOptions[\PDO::ATTR_ERRMODE] = \PDO::ERRMODE_EXCEPTION;
        return $this->PDOptions;
    }

    #prevent properties from showing in var_dump and print_r for additional security
    public function __debugInfo(): array
    {
        return [];
    }
}
