# polyclay-rethink

Rethinkdb backing storage for [polyclay](https://github.com/ceejbot/polyclay) models

[![on npm](http://img.shields.io/npm/v/polyclay-rethink.svg?style=flat)](https://www.npmjs.org/package/polyclay-rethink)  [![Tests](http://img.shields.io/travis/ceejbot/polyclay-rethink.svg?style=flat)](http://travis-ci.org/ceejbot/polyclay-rethink) ![Coverage](http://img.shields.io/badge/coverage-100%25-green.svg?style=flat) [![Dependencies](http://img.shields.io/david/ceejbot/polyclay-rethink.svg?style=flat)](https://david-dm.org/ceejbot/polyclay-rethink)

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
    host:      'localhost',
    port:      28015,
    authKey:   'optional auth key',
    database:  'test', // required
    tablename: 'widget_table', // optional
    dbopts:    { }, // optional
};
Widget.setStorage(options, Adapter);
```

If necessary, the adapter will create the database and the table named for the model. If `tablename` isn't provided, the model's plural will be used instead.

If you need to specify options for table sharding, set them in `dbopts`. They'll be passed to `rethink.tableCreate()`.

## Secondary indexes

To create a secondary index on a property, add the name of the property to the `index` option in the model builder. This option must be an array.

In the example above, a secondary index is created for the `name` property. A finder named `byName()` is created on the model constructor. This function will always start with `by` and then add the name of the property with the first letter upper-cased. E.g., if you create an index for a property named `snake_case`, you'll get a finder named `bySnake_case`, which will be silly-looking but is at least predictable. The alias `findByProperty()` is also created, for historical reasons.

```javascript
Widget.byName('flux capacitor', function(err, widgets)
{
    apply21Gigawatts(widgets[0]);
});
```



## TODO

Make the finders flexible about promises vs arrays.
