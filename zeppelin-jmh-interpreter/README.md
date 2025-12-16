# zeppelin-jmh-interpreter
JMH interpreter for Apache Zeppelin.


## Build

```sh
./gradlew build
```

## Deployment

* Update `$ZEPPELIN_HOME/conf/zeppeln-site.xml`
```xml
<property>'
  <name>zeppelin.interpreters</name>
  <value>...,org.apache.jmh.JMHInterpreter</value>
</property>
```
* Create `$ZEPPELIN_HOME/interpreter/jmh`
* Copy interpreter jar in `$ZEPPELIN_HOME/interpreter/jmh`


## Configuration

TODO
<table>
  <tr><th>Parameter</th><th>Default value</th><th>Description</th></tr>
</table>

## How to use

In Zeppelin, use `%jmh` in a paragraph.


Examples:
```javascript
%jmh

// Display a table
db.zipcodes.find({ "city":"CHICAGO", "state": "IL" }).table()
```

## Examples




