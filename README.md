# polyclay-rethink

Rethinkdb backing storage for [polyclay](https://github.com/ceejbot/polyclay) models

[![on npm](http://img.shields.io/npm/v/numbat-emitter.svg?style=flat)](https://www.npmjs.org/package/polyclay-rethink)  [![Tests](http://img.shields.io/travis/ceejbot/polyclay-rethink.svg?style=flat)](http://travis-ci.org/ceejbot/polyclay-rethink) ![Coverage](http://img.shields.io/badge/coverage-97%25-green.svg?style=flat) [![Dependencies](http://img.shields.io/david/ceejbot/polyclay-rethink.svg?style=flat)](https://david-dm.org/ceejbot/polyclay-rethink)

## How-to

```javascript
var polyclay = require('polyclay'),
    Adapter = require('polyclay-rethink');

var Widget = polyclay.Model.buildClass({
    properties:
    {
        partnum: 'string'
        name: 'string',
        description: 'string',
    },
    singular: 'widget',
    plural: 'widgets',
    index: [ 'name' ]
});
polyclay.persist(Widget, 'partnum');

var options =
{
    host:     'localhost',
    port:     28015,
    database: 'test',
};
Widget.setStorage(options, Adapter);
```
