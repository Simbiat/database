<?php
#Supressing cohesion and too many members inspection, since it does not make sense to split them into something separate
/** @noinspection PhpClassHasTooManyDeclaredMembersInspection */
/** @noinspection PhpLackOfCohesionInspection */
declare(strict_types = 1);

namespace Simbiat\Database;

use JetBrains\PhpStorm\Pure;
use function in_array;

/**
 * Database configuration
 */
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
    
    /**
     * Set database user
     * @param string $user
     *
     * @return $this
     */
    public function setUser(#[\SensitiveParameter] string $user): self
    {
        if (empty($user)) {
            throw new \InvalidArgumentException('Attempted to set empty user.');
        }
        $this->user = $user;
        return $this;
    }
    
    /**
     * Get current database user
     * @return string
     */
    public function getUser(): string
    {
        return (empty($this->user) ? '' : $this->user);
    }
    
    /**
     * Set database password
     * @param string $password
     *
     * @return $this
     */
    public function setPassword(#[\SensitiveParameter] string $password = ''): self
    {
        $this->password = $password;
        return $this;
    }
    
    /**
     * Get current password. Access allowed only for `openConnection` method from `Pool` class
     * @return string
     */
    public function getPassword(): string
    {
        #Restricting direct access to password for additional security
        $caller = debug_backtrace();
        if (empty($caller[1])) {
            throw new \RuntimeException('Direct call detected. Access denied.');
        }
        $caller = $caller[1];
        if ($caller['function'] !== 'openConnection' || $caller['class'] !== 'Simbiat\\Database\\Pool') {
            throw new \RuntimeException('Call from non-allowed function or object-type detected. Access denied.');
        }
        return (empty($this->password) ? '' : $this->password);
    }
    
    /**
     * Set database host
     * @param string      $host   Host IP or DNS name
     * @param int|null    $port   Host port if not default
     * @param string|null $socket Host socket, if any
     *
     * @return $this
     */
    public function setHost(string $host = 'localhost', int $port = NULL, string $socket = NULL): self
    {
        $this->host = (empty($host) ? 'localhost' : $host);
        $this->port = ($port < 1 || $port > 65535 ? NULL : $port);
        $this->socket = $socket;
        return $this;
    }
    
    /**
     * Get current host setup
     * @return string
     */
    public function getHost(): string
    {
        if (empty($this->socket)) {
            return 'host='.$this->host.';'.(empty($this->port) ? '' : 'port='.$this->port.';');
        }
        return 'unix_socket='.$this->socket.';';
    }
    
    /**
     * Set what PDO driver to use
     * @param string $driver
     *
     * @return $this
     */
    public function setDriver(string $driver = 'mysql'): self
    {
        if (in_array($driver, \PDO::getAvailableDrivers(), true)) {
            $this->driver = $driver;
        } else {
            throw new \InvalidArgumentException('Attempted to set unsupported driver.');
        }
        return $this;
    }
    
    /**
     * Get current driver
     * @return string
     */
    public function getDriver(): string
    {
        return (empty($this->driver) ? '' : $this->driver);
    }
    
    /**
     * Set database name
     * @param string $dbname
     *
     * @return $this
     */
    public function setDB(string $dbname): self
    {
        if (empty($dbname)) {
            throw new \InvalidArgumentException('Attempted to set empty database name.');
        }
        $this->dbname = $dbname;
        return $this;
    }
    
    /**
     * Get current database name
     * @return string
     */
    public function getDB(): string
    {
        return (empty($this->dbname) ? '' : 'dbname='.$this->dbname.';');
    }
    
    /**
     * Set characters set. If empty string, will force utf8mb4.
     * @param string $charset
     *
     * @return $this
     */
    public function setCharset(string $charset): self
    {
        $this->charset = (empty($charset) ? 'utf8mb4' : $charset);
        return $this;
    }
    
    /**
     * Get current character set
     * @return string
     */
    public function getCharset(): string
    {
        return (empty($this->charset) ? '' : 'charset='.$this->charset.';');
    }
    
    /**
     * Set application name (for DB-Lib only)
     * @param string $appName
     *
     * @return $this
     */
    public function setAppName(string $appName): self
    {
        $this->appName = (empty($appName) ? 'PHP Generic DB-lib' : $appName);
        return $this;
    }
    
    /**
     * Get current application name (for DB-Lib only)
     * @return string
     */
    public function getAppName(): string
    {
        return (empty($this->appName) ? '' : 'appname='.$this->appName.';');
    }
    
    /**
     * Set role (for Firebird only)
     * @param string $role
     *
     * @return $this
     */
    public function setRole(string $role): self
    {
        $this->role = (empty($role) ? null : $role);
        return $this;
    }
    
    /**
     * Get current role (for Firebird only)
     * @return string
     */
    public function getRole(): string
    {
        return (empty($this->role) ? '' : 'role='.$this->role.';');
    }
    
    /**
     * Set dialect (for Firebird only)
     * @param int $dialect
     *
     * @return $this
     */
    public function setDialect(int $dialect): self
    {
        if ($dialect !== 0 && $dialect !== 3) {
            $dialect = 3;
        }
        $this->dialect = $dialect;
        return $this;
    }
    
    /**
     * Get current dialect (for Firebird only)
     * @return string
     */
    public function getDialect(): string
    {
        return 'dialect='.$this->dialect.';';
    }
    
    /**
     * Set SSL mode (for PostgresSQL only)
     * @param string $sslmode
     *
     * @return $this
     */
    public function setSSLMode(string $sslmode): self
    {
        if (!in_array($sslmode, ['disable', 'allow', 'prefer', 'require', 'verify-ca', 'verify-full'])) {
            $sslmode = 'verify-full';
        }
        $this->sslmode = $sslmode;
        return $this;
    }
    
    /**
     * Get current SSL mode (for PostgresSQL only)
     * @return string
     */
    public function getSSLMode(): string
    {
        return 'sslmode='.$this->sslmode.';';
    }
    
    /**
     * Set custom connection string. `Password`, `Pass`, `PWD`, `UID`, `User ID`, `User`, `Username` fields will be stripped if present.
     * @param string $customString
     *
     * @return $this
     */
    public function setCustomString(string $customString): self
    {
        #Remove username and password values
        $customString = preg_replace('/(Password|Pass|PWD|UID|User ID|User|Username)=[^;]+;/miu', '', $customString);
        $this->customString = $customString;
        return $this;
    }
    
    /**
     * Get current custom connection string
     * @return string
     */
    public function getCustomString(): string
    {
        return $this->customString;
    }
    
    /**
     * Get IBM specific connection string
     * @return string
     */
    public function getIBM(): string
    {
        $dbname = $this->getDB();
        if (preg_match('/.+\.ini$/ui', $dbname)) {
            return $dbname;
        }
        return 'DRIVER={IBM DB2 ODBC DRIVER};DATABASE='.$dbname.';HOSTNAME='.$this->host.';'.(empty($this->port) ? '' : 'PORT='.$this->port.';').'PROTOCOL=TCPIP;';
    }
    
    /**
     * Get Informix specific connection string
     * @return string
     */
    public function getInformix(): string
    {
        return 'host='.$this->host.';'.(empty($this->port) ? '' : 'service='.$this->port.';').'database='.$this->dbname.';protocol=onsoctcp;EnableScrollableCursors=1;';
    }
    
    /**
     * Get database name in a way compliant with SQLLite, that is either `:memory`, path to a file (if it exists) or empty string (temporary database).
     * @return string
     */
    #[Pure(true)] public function getSQLLite(): string
    {
        $dbname = $this->getDB();
        #Check if we are using in-memory DB
        if ($dbname === ':memory:') {
            return $dbname;
        }
        #Check if it's a file that exists
        if (is_file($dbname)) {
            return $dbname;
        }
        #Assume temporary database
        return '';
    }
    
    /**
     * Get database name for ODBC
     * @return string
     */
    public function getODBC(): string
    {
        return $this->dbname ?? '';
    }
    
    /**
     * Get connection string for MS SQL Server
     * @return string
     */
    public function getSQLServer(): string
    {
        return 'Server='.$this->host.(empty($this->port) ? '' : ','.$this->port).';Database='.$this->dbname;
    }
    
    /**
     * Get Data Source Name (DSN) string based on current settings
     * @return string
     */
    public function getDSN(): string
    {
        $dsn = match ($this->getDriver()) {
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
        }
        throw new \UnexpectedValueException('Unsupported driver.');
    }
    
    /**
     * Set custom options to use during establishing connection
     * @param int   $option Appropriate `\PDO::*` constant
     * @param mixed $value  Value for the option
     *
     * @return $this
     */
    public function setOption(int $option, mixed $value): self
    {
        if (
            in_array($option, [\PDO::ATTR_ERRMODE, \PDO::ATTR_EMULATE_PREPARES], true)
            ||
            ($this->getDriver() === 'sqlsrv' && $option === \PDO::SQLSRV_ATTR_DIRECT_QUERY)
            ||
            ($this->getDriver() === 'mysql' && in_array($option, [\PDO::MYSQL_ATTR_MULTI_STATEMENTS, \PDO::MYSQL_ATTR_DIRECT_QUERY, \PDO::MYSQL_ATTR_IGNORE_SPACE, \PDO::MYSQL_ATTR_USE_BUFFERED_QUERY], true))
        ) {
            throw new \InvalidArgumentException('Attempted to set restricted attribute.');
        }
        $this->PDOptions[$option] = $value;
        return $this;
    }
    
    /**
     * Get current set of custom options. Certain options will be forced depending on driver for security and compatibility reasons.
     * @return array
     */
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
    
    /**
     * Prevent properties from showing in var_dump and print_r for additional security
     * @return array
     */
    public function __debugInfo(): array
    {
        return [];
    }
}
