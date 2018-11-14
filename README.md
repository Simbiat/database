# Database Pool and Controller
This is a set of 3 classes for convinience of work with databases, a wrapper and sugar for PHP's PDO.

## Classes
- Config: A class for preparing configuration for database connection. You can treat this as an alternative to common 'config.php' files, but it has a few potentially benefitial features:
  - Enforcing of useful security benefitial driver settings
  - Validation of some host parameters (like port number)
  - Convinient DSN generation using `getDSN()` function
  - Password protection that makes it a bit harder to spoof the password from outside functions, besides the appropriate Pool class
- Pool: A static proxy class which can pool database connection setups (\SimbiatDB\Config objects) and use the one currently active, when you are requesting a PDO connection. With ability to change active connection, if required.
- Controller: a wrapper with some potentially useful features:
  - You can send both string (single query) and array (set of queries) and both will be processed. In case of array, it will automatically start a transaction and process each query separately. In case array will have any SELECT-like queries you will be notified, because thier output may not get processed properly.
  - Attempts to retry in case of deadlock. You can set number of retries and time to sleep before each try using appropirate setters.
  - Binding sugar:
    - Instead of `\PDO::PARAM_*` or obscure integers you can send appropriate types as strings, like `'boolean'`
    - Enforced type casting: it's known, that sometimes PDO driver does not cast or casts wierdly. Some of the cases enforce regular casting in attempt to work around this
    - Limit casting: when sending binding for use with LIMIT you need to remember to cast it as integer, but with this controller - just mark it as `'limit'`
    - Like casting: when sending binding for use with LIKE  you need to enclose it in `%` yourself, but with this contoller - just mark it as `'like'`
    - Date and time casting: mark a binding as `'date'` or `'time'` and script will attempt to check if it's a datetime value and convert it to an appropriate string for you. Or even get current time, if you do not want to use engine's own functions
  - Semantical wrappers: a set of functions for SELECTs which clearly state, what they will be returning (row, column, everything)
  - Smart result return: if you send a single query, script will attempt to identify what it is and do either `fetch()` or `fetchAll()` or `rowCount()` and setting appropriate result. Drawback is that you need to use `getResult()` afterwards.

## How to use
###### *Please, note, that I am using MySQL as main DB engine in my projects, thus I may miss some peculiarities of other engines. Please, let me know of them, so that they can be incorporated.*

*to be filled*

If you are using a set of connections, I recommend sending your own IDs on connections creation for easse of use, otherwise, they will be generated automatically.
