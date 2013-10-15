lei-mysql [![Dependencies Status](https://david-dm.org/leizongmin/node-lei-ns.png)](http://david-dm.org/leizongmin/node-lei-ns)
=======

使用方法：

```javascript
var MySQLPool = require('peento-module-mysql-pool');

var pool = new MySQLPool({
  host:     '127.0.0.1',
  port:     3306,
  database: 'test',
  user:     'root',
  password: '12345'
  pool:     10
});
```

详细使用方法请查看测试文件：https://github.com/leizongmin/node-lei-mysql/blob/master/test/test.js


License
========

```
Copyright (c) 2013 Zongmin Lei(雷宗民) <leizongmin@gmail.com>
http://ucdok.com

The MIT License

Permission is hereby granted, free of charge, to any person obtaining
a copy of this software and associated documentation files (the
"Software"), to deal in the Software without restriction, including
without limitation the rights to use, copy, modify, merge, publish,
distribute, sublicense, and/or sell copies of the Software, and to
permit persons to whom the Software is furnished to do so, subject to
the following conditions:

The above copyright notice and this permission notice shall be
included in all copies or substantial portions of the Software.

THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND,
EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF
MERCHANTABILITY, FITNESS FOR A PARTICULAR PURPOSE AND
NONINFRINGEMENT. IN NO EVENT SHALL THE AUTHORS OR COPYRIGHT HOLDERS BE
LIABLE FOR ANY CLAIM, DAMAGES OR OTHER LIABILITY, WHETHER IN AN ACTION
OF CONTRACT, TORT OR OTHERWISE, ARISING FROM, OUT OF OR IN CONNECTION
WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE SOFTWARE.
```