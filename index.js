var
	_       = require('lodash'),
	assert  = require('assert'),
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
	assert(opts && _.isObject(opts), 'you must pass an options object');
	assert(opts.database && _.isString(opts.database), 'you must pass a database name in opts.database');
	assert(modelfunc && _.isFunction(modelfunc), 'you must pass a model constructor as the second argument');

	this.options = opts;
	this.tablename = opts.dbname || opts.tablename || modelfunc.prototype.plural;
	this.constructor = modelfunc;

	this.Rethink = Rethink;
	this.db = Rethink.db(this.options.database);
	this.objects = Rethink.table(this.tablename);
	this.attachments = Rethink.table(this.tablename + '_attachments');

	if (modelfunc.prototype.__index)
	{
		var self = this;
		var indexes = modelfunc.prototype.__index;

		_.each(indexes, function(property)
		{
			var getter = 'by' + property[0].toUpperCase() + property.substr(1);
			modelfunc[getter] = function(value, callback)
			{
				self.getAllBy(property, value, callback);
			};
			var alias = 'find' + getter[0].toUpperCase() + getter.substr(1);
			modelfunc[alias] = modelfunc[getter];
		});
	}

	this.connect();
};

RethinkAdapter.prototype.connect = function()
{
	var self = this;

	var opts = {
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
	}).done();
};

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

RethinkAdapter.prototype._createIndex = function _createIndex(index, callback)
{
	var self = this;

	self.objects.indexList()
	.run(self.connection, function(err, indices)
	{
		if (err) return callback(err);
		if (indices.indexOf(index) > -1)
			return callback();

		self.objects.indexCreate(index).run(self.connection, callback);
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
		self._createTable(self.tablename + '_attachments', makeIndices);
	}

	function makeIndices()
	{
		var indexes = self.constructor.prototype.__index;
		var actions = _.map(indexes, function(property)
		{
			var f = function(cb) { self._createIndex(property, cb); };
			return f;
		});
		async.parallel(actions, function(err, results)
		{
			callback(err);
		});
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
		throw new Error('cannot save a document without a key');

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

RethinkAdapter.prototype.getAllBy = function(index, value, callback)
{
	var self = this;
	self.objects
	.getAll(value, {index: index})
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
	var self = this;

	if (!this.connection) return callback();
	this.connection.close(function()
	{
		self.connection = null;
		callback();
	});
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
