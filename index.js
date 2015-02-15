var
    _       = require('lodash'),
    async   = require('async'),
    events  = require('events'),
    Rethink = require('rethinkdb'),
    util    = require('util')
    ;

var RethinkAdapter = module.exports = function RethinkAdapter()
{
    events.EventEmitter.call(this);
};
util.inherits(RethinkAdapter, events.EventEmitter);

RethinkAdapter.prototype.connection  = null;
RethinkAdapter.prototype.dbname      = null;
RethinkAdapter.prototype.constructor = null;
RethinkAdapter.prototype.options     = null;
RethinkAdapter.prototype.attempts    = 0;

RethinkAdapter.prototype.configure = function(opts, modelfunc)
{
    this.options = opts;
    this.tablename = opts.dbname || modelfunc.prototype.plural;
    this.constructor = modelfunc;

    this.connect();
};

RethinkAdapter.prototype.connect = function()
{
    var self = this;

    var opts =
    {
        host:    this.options.host || 'localhost',
        port:    this.options.port || 28015,
        authKey: this.options.authKey,
        db:      this.options.database,
    };

    Rethink.connect(opts)
    .then(function(conn)
    {
        self.connection = conn;
        self.emit('ready');
    })
    .error(function(err)
    {
        console.log(err);
        self.emit('error', err);
    }).done();
};

function exponentialBackoff(attempt)
{
    return Math.min(Math.floor(Math.random() * Math.pow(2, attempt) + 10), 10000);
}

RethinkAdapter.prototype._createTable = function _createTable(tname, callback)
{
    var self = this;

    self.db.tableList()
    .run(self.connection, function(err, tables)
    {
        if (err) return callback(err);
        if (tables.indexOf(tname) > -1)
            return callback();

        var tableOpts = self.options.dbopts || {};
        tableOpts.primaryKey = self.constructor.prototype.keyfield;
        self.db.tableCreate(tname, tableOpts).run(self.connection, callback);
    });
};

RethinkAdapter.prototype.provision = function(callback)
{
    var self = this;
    self.db = Rethink.db(self.options.database);
    self.objects = Rethink.table(self.tablename);
    self.attachments = Rethink.table(self.tablename + '_attachments');

    // TODO cleanup-- yuck
    function makeAttachments()
    {
        self._createTable(self.tablename + '_attachments', callback);
    }

    Rethink.dbList().run(self.connection, function(err, dbs)
    {
        if (err) return callback(err);
        if (dbs.indexOf(self.options.database) > -1)
            return self._createTable(self.tablename, makeAttachments);
        Rethink.dbCreate(self.options.database)
        .run(self.connection, function(err, ignored)
        {
            if (err) return callback(err);
            self._createTable(self.tablename, makeAttachments);
        });
    });
};

RethinkAdapter.prototype.all = function all(callback)
{
    var self = this;

    self.objects
        .orderBy(self.constructor.prototype.keyfield)
        .run(self.connection, function(err, cursor)
    {
        if (err) return callback(err);

        cursor.toArray(function(err, results)
        {
            if (err) return callback(err);
            if (!results || !results.length) return callback(null, [], true);

            var batched = _.map(results, function(json)
            {
                var obj = new self.constructor();
                obj.initFromStorage(json);
                return obj;
            });

            cursor.close();
            callback(null, batched, true);
        });
    });
};

RethinkAdapter.prototype.save = function save(object, json, callback)
{
    if (!object.key || !object.key.length)
        throw(new Error('cannot save a document without a key'));

    var self = this;
    var payload = RethinkAdapter.flatten(json);

    self.objects.insert(payload.body).run(self.connection, function(err, res)
    {
        if (err) return callback(err);
        if (!payload.attachments.length)
            callback(null, 'OK');

        var actions = _.map(payload.attachments, function(v)
        {
            return function(cb) { self.saveAttachment(object, v, cb); };
        });

        async.parallel(actions, function(err, res)
        {
            callback(err, 'OK');
        });
    });
};

RethinkAdapter.prototype.update = function(object, json, callback)
{
    var self = this;
    var payload = RethinkAdapter.flatten(json);

    self.objects
        .get(object.key)
        .update(payload.body)
        .run(self.connection, function(err, res)
    {
        if (err) return callback(err);
        if (!payload.attachments.length)
            callback(null, 'OK');

        var actions = _.map(payload.attachments, function(v)
        {
            return function(cb) { self.saveAttachment(object, v, cb); };
        });

        async.parallel(actions, function(err, res)
        {
            callback(err, 'OK');
        });
    });
};

RethinkAdapter.prototype.get = function(key, callback)
{
    var self = this;
    if (Array.isArray(key))
        return this.getBatch(key, callback);

    self.objects.get(key).run(self.connection, function(err, json)
    {
        if (err) return callback(err);
        if (!json) return callback();

        var object = new self.constructor();
        object.initFromStorage(json);
        callback(null, object);
    });
};

RethinkAdapter.prototype.getBatch = function(keylist, callback)
{
    var self = this;

    self.objects
        .getAll(Rethink.args(keylist))
        .coerceTo('array')
        .run(self.connection, function(err, results)
    {
        if (err) return callback(err);
        if (!results || !results.length) return callback(null, []);

        var batched = _.map(results, function(json)
        {
            var obj = new self.constructor();
            obj.initFromStorage(json);
            return obj;
        });

        callback(null, batched);
    });
};

RethinkAdapter.prototype.merge = function(key, attributes, callback)
{
    var self = this;
    self.objects.get(key).update(attributes).run(self.connection, callback);
};

RethinkAdapter.prototype.remove = function(object, callback)
{
    if (!object.key || !object.key.length)
        throw(new Error('cannot delete a document without a key'));

    var self = this;
    self.objects.get(object.key).delete().run(self.connection, callback);
};

RethinkAdapter.prototype.destroyMany = function(objects, callback)
{
    var self = this;
    var ids = _.map(objects, function(obj)
    {
        if (typeof obj === 'string')
            return obj;
        return obj.key;
    });

    self.objects
        .getAll(Rethink.args(ids))
        .delete({returnChanges: false})
        .run(self.connection, callback);
};

var makeAttachmentKey = RethinkAdapter.makeAttachmentKey = function makeAttachmentKey(key, name)
{
    return key + ':' + name;
};

RethinkAdapter.prototype.attachment = function(key, name, callback)
{
    var self = this;
    var akey = makeAttachmentKey(key, name);
    self.attachments.get(akey).run(self.connection, function(err, attach)
    {
        if (err) return callback(err);
        callback(null, attach.body);
    });
};

RethinkAdapter.prototype.saveAttachment = function(object, attachment, callback)
{
    var self = this;
    var akey = makeAttachmentKey(object.key || object, attachment.name);

    // TODO this seems horrible. is there a better way?
    self.attachments.get(akey).run(self.connection, function(err, existing)
    {
        if (err) return callback(err);
        if (!existing)
        {
            attachment[self.constructor.prototype.keyfield] = akey;
            self.attachments.insert(attachment).run(self.connection, callback);
        }
        else
        {
            self.attachments.get(akey).update(attachment).run(self.connection, function(err, res)
            {
                callback(err);
            });
        }
    });
};

RethinkAdapter.prototype.removeAttachment = function(object, name, callback)
{
    var self = this;
    var akey = makeAttachmentKey(object.key, name);
    self.objects.get(akey).delete().run(self.connection, callback);
};

RethinkAdapter.prototype.shutdown = function(callback)
{
    if (!this.connection) return callback();
    this.connection.close(callback);
};

RethinkAdapter.flatten = function(json)
{
    var payload = {};
    payload.attachments = [];

    if (json._attachments)
    {
        var attaches = Object.keys(json._attachments);
        for (var i = 0; i < attaches.length; i++)
        {
            var attachment = json._attachments[attaches[i]];
            payload.attachments.push({
                name: attaches[i],
                body: attachment.body
            });
        }
        delete json._attachments;
    }

    payload.body = _.clone(json);
    return payload;
};
