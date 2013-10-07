lei-ns [![Build Status](https://secure.travis-ci.org/leizongmin/node-lei-ns.png?branch=master)](http://travis-ci.org/leizongmin/node-lei-ns) [![Dependencies Status](https://david-dm.org/leizongmin/node-lei-ns.png)](http://david-dm.org/leizongmin/node-lei-ns)
=======

使用方法：

```javascript
var ns = require('lei-ns');
    
// 设置
console.log(ns('test.abc', {a: 123}));
// 读取, 如果不存在则返回undefined
console.log(ns('test.abc'));

// 设置一个对象到顶级命名空间
console.log(ns({a: 123, b: 456}));
// 读取顶级命名空间
console.log(ns());

// 创建非公共的命名空间
var myns = ns.Namespace();
console.log(ns('test.abc', {a: 123}));
console.log(ns('test.abc'));
```

说明：

* 名称使用小数点 `.` 来进行分隔


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