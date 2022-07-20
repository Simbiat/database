# Database Pool and Controller
This is a set of 3 classes for convenience of work with databases, a wrapper and sugar for PHP's PDO.

## Classes
- Config: A class for preparing configuration for database connection. You can treat this as an alternative to common 'config.php' files, but it has a few potentially beneficial features:
  - Enforcing of useful security beneficial driver settings
  - Validation of some host parameters (like port number)
  - Convenient DSN generation using `getDSN()` function
  - Password protection that makes it a bit harder to spoof the password from outside functions, besides the appropriate Pool class
- Pool: A static proxy class which can pool database connection setups (\Simbiat\Database\Config objects) and use the one currently active, when you are requesting a PDO connection. With ability to change active connection, if required.
- Controller: a wrapper with some potentially useful features:
  - You can send both string (single query) and array (set of queries) and both will be processed. In case of array, it will automatically start a transaction and process each query separately. In case array will have any SELECT-like queries you will be notified, because their output may not get processed properly.
  - Attempts to retry in case of deadlock. You can set number of retries and time to sleep before each try using appropriate setters.
  - Binding sugar:
    - Instead of `\PDO::PARAM_*` or obscure integers you can send appropriate types as strings, like `'boolean'`
    - Enforced type casting: it's known, that sometimes PDO driver does not cast or casts weirdly. Some cases enforce regular casting in attempt to work around this
    - Limit casting: when sending binding for use with LIMIT you need to remember to cast it as integer, but with this controller - just mark it as `'limit'`
    - Like casting: when sending binding for use with LIKE  you need to enclose it in `%` yourself, but with this controller - just mark it as `'like'`
    - Date and time casting: mark a binding as `'date'` or `'time'` and script will attempt to check if it's a datetime value and convert it to an appropriate string for you. Or even get current time, if you do not want to use engine's own functions
  - Semantic wrappers: a set of functions for SELECT which clearly state, what they will be returning (row, column, everything)
  - Smart result return: if you send a single query, script will attempt to identify what it is and do either `fetch()` or `fetchAll()` or `rowCount()` and setting appropriate result. Drawback is that you need to use `getResult()` afterwards.

## How to use
###### *Please, note, that I am using MySQL as main DB engine in my projects, thus I may miss some peculiarities of other engines. Please, let me know of them, so that they can be incorporated.*

First you need to create a \Config object and set its parameters like this:
```php
$config = (new \Simbiat\Database\Config)->setUser('user')->setPassword('password')->setDB('database');
```
The above line is the minimum you will require. Additionally, you can set driver options like this:
```php
$config->setOption(\PDO::MYSQL_ATTR_FOUND_ROWS, true)->setOption(\PDO::MYSQL_ATTR_INIT_COMMAND, 'SET @@global.character_set_client = \'utf8mb4\', @@global.character_set_connection = \'utf8mb4\', @@global.character_set_database = \'utf8mb4\', @@global.character_set_results = \'utf8mb4\', @@global.character_set_server = \'utf8mb4\', @@global.time_zone=\'+00:00\'');
```
After you set it up to your liking (can be done in one line), you need to add it to pool:
```php
(new \Simbiat\Database\Pool)->openConnection($config, 'example');
```
Passing ID is not required but will improve your life if you will need to juggle multiple connections.

You can also pass the third argument (`maxTries`), if you want to have the script to retry connection, in case of failures. Default is `1`.

If connection is established successfully you then can get \PDO object for it by not sending any parameters to the pool:
```php
(new \Simbiat\Database\Pool)->openConnection();
```
Or sending a previous `$config` or connection ID if you need a specific one. \Controller class uses variant without parameters for flexibility, but can be overridden, if deemed necessary.

If connection failed, you will be able to get errors using
```php
\Simbiat\Database\Pool::$errors
```

To utilize \Controller you need to establish as shown above and then call either it `query()` function or any of the wrappers. For example, this line will count rows in a table and return only the number of those rows, that is an integer:
```php
(new \Simbiat\Database\Controller)->count('SELECT COUNT(*) FROM `table`');
```
This one will return a boolean, advising if something exists in a table:
```php
(new \Simbiat\Database\Controller)->check('SELECT * FROM `table` WHERE `time`=:value', [':value'=>['', 'time']]);
```
The above example also shows one of possible ways to set bindings. Regular \PDO allows binding values like `hook, value, type`, but \Controller expects an array for `value` if you want to send a non-string one or a special value, like the above mentioned `time`. Since We are sending an empty value for `time` \Controller will take current microtime and convert and bind it in `Y-m-d H:i:s.u` format.
