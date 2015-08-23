/*global describe:true, it:true, before:true, after:true */

var
	demand   = require('must'),
	events   = require('events'),
	fs       = require('fs'),
	path     = require('path'),
	polyclay = require('polyclay'),
	Rethink  = require('rethinkdb'),
	Adapter  = require('../index')
;

var testDir = process.cwd();
if (path.basename(testDir) !== 'test')
	testDir = path.join(testDir, 'test');
var attachmentdata = fs.readFileSync(path.join(testDir, 'test.png'));

describe('rethinkdb adapter', function()
{
	var modelDefinition =
	{
		properties:
		{
			key:           'string',
			name:          'string',
			created:       'date',
			foozles:       'array',
			snozzers:      'hash',
			is_valid:      'boolean',
			count:         'number',
			required_prop: 'string',
			ttl:           'number'
		},
		optional: [ 'computed', 'ephemeral' ],
		required: [ 'name', 'is_valid', 'required_prop'],
		singular: 'model',
		plural: 'models',
		index: [ 'name' ],
		initialize: function()
		{
			this.ran_init = true;
		}
	};

	var Model, instance, another, hookTest, hookid;

	before(function()
	{
		Model = polyclay.Model.buildClass(modelDefinition);
		polyclay.persist(Model);
	});

	it('can be configured for database access', function(done)
	{
		var options =
		{
			host:     'localhost',
			port:     28015,
			database: 'test',
		};

		Model.setStorage(options, Adapter);
		Model.adapter.must.exist();
		Model.adapter.constructor.must.equal(Model);
		Model.adapter.tablename.must.equal(Model.prototype.plural);
		Model.adapter.once('ready', function()
		{
			Model.adapter.must.have.property('connection');
			Model.adapter.connection.must.be.an.object();
			done();
		});
		Model.adapter.on('error', function(err)
		{
			throw(err);
		});
	});

	it('configure() throws without an options object', function()
	{
		function shouldThrow()
		{
			var adapter = new Adapter();
			adapter.configure();
		}

		shouldThrow.must.throw(/options object/);
	});

	it('configure() throws without a database name option', function()
	{
		function shouldThrow()
		{
			var adapter = new Adapter();
			adapter.configure({});
		}

		shouldThrow.must.throw(/database/);
	});

	it('configure() throws without a model constructor', function()
	{
		function shouldThrow()
		{
			var adapter = new Adapter();
			adapter.configure({database: 'test'});
		}

		shouldThrow.must.throw(/constructor/);
	});

	it('configure() creates finder functions', function()
	{
		Model.must.have.property('byName');
		Model.byName.must.be.a.function();
		Model.must.have.property('findByName');
		Model.findByName.must.be.a.function();
	});

	it('provision() creates the database if necessary', function(done)
	{
		this.timeout(8000);
		var c = Model.adapter.connection;

		Model.provision(function(err)
		{
			demand(err).not.exist();
			Rethink.dbList().run(c, function(err, dbs)
			{
				demand(err).not.exist();
				dbs.must.be.an.array();
				dbs.indexOf('test').must.be.above(-1);
				done();
			});
		});
	});

	it('provision() creates the table if necessary', function(done)
	{
		var c = Model.adapter.connection;
		Model.adapter.db
		.tableList()
		.run(c, function(err, tables)
		{
			demand(err).not.exist();
			tables.must.be.an.array();
			tables.indexOf('models').must.be.above(-1);
			done();
		});
	});

	it('provision() creates requested indexes', function(done)
	{
		Model.adapter.objects.indexList()
		.run(Model.adapter.connection, function(err, indices)
		{
			demand(err).not.exist();
			indices.indexOf('name').must.be.above(-1);
			done();
		});
	});

	it('provision() can be called more than once safely', function(done)
	{
		Model.provision(function(err)
		{
			demand(err).not.exist();
			done();
		});
	});

	it('save() throws when asked to save a document without a key', function()
	{
		var noID = function()
		{
			var obj = new Model();
			obj.name = 'idless';
			obj.save(function(err, reply) {});
		};

		noID.must.throw(Error);
	});

	it('save() can save a document in the db', function(done)
	{
		instance = new Model();
		instance.update(
		{
			key: '1',
			name: 'test',
			created: Date.now(),
			foozles: ['three', 'two', 'one'],
			snozzers: { field: 'value' },
			is_valid: true,
			count: 3,
			required_prop: 'requirement met',
			computed: 17
		});

		instance.save(function(err, reply)
		{
			demand(err).not.exist();
			reply.must.exist();
			done();
		});
	});

	it('Model.get() can retrieve the saved document', function(done)
	{
		Model.get(instance.key, function(err, retrieved)
		{
			demand(err).not.exist();
			retrieved.must.exist();
			retrieved.must.be.an.object();
			retrieved.key.must.equal(instance.key);
			retrieved.name.must.equal(instance.name);
			retrieved.created.getTime().must.equal(instance.created.getTime());
			retrieved.is_valid.must.equal(instance.is_valid);
			retrieved.count.must.equal(instance.count);
			retrieved.computed.must.equal(instance.computed);
			done();
		});
	});

	it('can update the document', function(done)
	{
		instance.name = 'New name';
		instance.isDirty().must.be.true();
		instance.save(function(err, response)
		{
			demand(err).not.exist();
			response.must.be.a.string();
			response.must.equal('OK');
			instance.isDirty().must.equal(false);
			done();
		});
	});

	it('can fetch in batches', function(done)
	{
		var ids = [ instance.key ];
		var obj = new Model();
		obj.name = 'two';
		obj.key = '2';
		obj.save(function(err, response)
		{
			ids.push(obj.key);

			Model.get(ids, function(err, itemlist)
			{
				demand(err).not.exist();
				itemlist.must.be.an.array();
				itemlist.length.must.equal(2);
				done();
			});
		});
	});

	it('get() can handle an id or an array of ids', function(done)
	{
		var ids = [ '1', '2' ];
		Model.adapter.get(ids, function(err, itemlist)
		{
			demand(err).not.exist();
			itemlist.must.be.an.array();
			itemlist.length.must.equal(2);
			done();
		});
	});

	it('can fetch all', function(done)
	{
		Model.all(function(err, itemlist)
		{
			demand(err).not.exist();
			itemlist.must.be.an.array();
			itemlist.length.must.be.at(2);
			done();
		});
	});

	it('constructMany() retuns an empty list when given empty input', function(done)
	{
		Model.constructMany([], function(err, results)
		{
			demand(err).not.exist();
			results.must.be.an.array();
			results.length.must.equal(0);
			done();
		});
	});

	it('merge() updates properties then saves the object', function(done)
	{
		Model.get('2', function(err, item)
		{
			demand(err).not.exist();

			item.merge({ is_valid: true, count: 1023 }, function(err, response)
			{
				demand(err).not.exist();
				Model.get(item.key, function(err, stored)
				{
					demand(err).not.exist();
					stored.count.must.equal(1023);
					stored.is_valid.must.equal(true);
					stored.name.must.equal(item.name);
					done();
				});
			});
		});
	});

	it('getAllBy() returns inflated objects from an index', function(done)
	{
		Model.adapter.getAllBy('name', 'two', function(err, objs)
		{
			demand(err).not.exist();
			objs.must.be.an.array();
			objs.length.must.equal(1);
			objs[0].name.must.equal('two');
			done();
		});
	});

	it('getAllBy() returns an empty list when there are no matches', function(done)
	{
		Model.adapter.getAllBy('name', 'notfound', function(err, objs)
		{
			demand(err).not.exist();
			objs.must.be.an.array();
			objs.length.must.equal(0);
			done();
		});
	});

	it('index finders return arrays of inflated objects', function(done)
	{
		Model.byName('two', function(err, objs)
		{
			demand(err).not.exist();
			objs.must.be.an.array();
			objs.length.must.equal(1);
			objs[0].name.must.equal('two');
			done();
		});
	});

	it('can add an attachment type', function()
	{
		Model.defineAttachment('frogs', 'text/plain');
		Model.defineAttachment('avatar', 'image/png');

		instance.set_frogs.must.be.a.function();
		instance.fetch_frogs.must.be.a.function();
		var property = Object.getOwnPropertyDescriptor(Model.prototype, 'frogs');
		property.get.must.be.a.function();
		property.set.must.be.a.function();
	});

	it('can save attachments', function(done)
	{
		instance.avatar = attachmentdata;
		instance.frogs = 'This is bunch of frogs.';
		instance.isDirty().must.equal.true;
		instance.save(function(err, response)
		{
			demand(err).not.exist();
			instance.isDirty().must.equal.false;
			done();
		});
	});

	it('can retrieve attachments', function(done)
	{
		Model.get(instance.key, function(err, retrieved)
		{
			retrieved.fetch_frogs(function(err, frogs)
			{
				demand(err).not.exist();
				frogs.must.be.a.string();
				frogs.must.equal('This is bunch of frogs.');
				retrieved.fetch_avatar(function(err, imagedata)
				{
					demand(err).not.exist();
					imagedata.must.be.instanceof(Buffer);
					imagedata.length.must.equal(attachmentdata.length);
					done();
				});
			});
		});
	});

	it('can update an attachment', function(done)
	{
		instance.frogs = 'Poison frogs are awesome.';
		instance.save(function(err, response)
		{
			demand(err).not.exist();
			Model.get(instance.key, function(err, retrieved)
			{
				demand(err).not.exist();
				retrieved.fetch_frogs(function(err, frogs)
				{
					demand(err).not.exist();
					frogs.must.equal(instance.frogs);
					retrieved.fetch_avatar(function(err, imagedata)
					{
						demand(err).not.exist();
						imagedata.length.must.equal(attachmentdata.length);
						done();
					});
				});
			});
		});
	});

	it('can store an attachment directly', function(done)
	{
		instance.frogs = 'Poison frogs are awesome, but I think sand frogs are adorable.';
		instance.saveAttachment('frogs', function(err, response)
		{
			demand(err).not.exist();
			Model.get(instance.key, function(err, retrieved)
			{
				demand(err).not.exist();
				retrieved.fetch_frogs(function(err, frogs)
				{
					demand(err).not.exist();
					frogs.must.equal(instance.frogs);
					done();
				});
			});
		});
	});

	it('saveAttachment() clears the dirty bit', function(done)
	{
		instance.frogs = 'This is bunch of frogs.';
		instance.isDirty().must.equal(true);
		instance.saveAttachment('frogs', function(err, response)
		{
			demand(err).not.exist();
			instance.isDirty().must.equal(false);
			done();
		});
	});

	it('can remove an attachment', function(done)
	{
		instance.removeAttachment('frogs', function(err, deleted)
		{
			demand(err).not.exist();
			deleted.must.be.true();
			done();
		});
	});

	it('caches an attachment after it is fetched', function(done)
	{
		instance.avatar = attachmentdata;
		instance.save(function(err, response)
		{
			demand(err).not.exist();
			instance.isDirty().must.be.false();
			instance.fetch_avatar(function(err, imagedata)
			{
				demand(err).not.exist();
				var cached = instance.__attachments.avatar.body;
				cached.must.exist();
				(cached instanceof Buffer).must.equal(true);
				polyclay.dataLength(cached).must.equal(polyclay.dataLength(attachmentdata));
				done();
			});
		});
	});

	it('can fetch an attachment directly', function(done)
	{
		Model.adapter.attachment('1', 'avatar', function(err, body)
		{
			demand(err).not.exist();
			body.must.be.instanceof(Buffer);
			polyclay.dataLength(body).must.equal(polyclay.dataLength(attachmentdata));
			done();
		});
	});

	it('removes an attachment when its data is set to null', function(done)
	{
		instance.avatar = null;
		instance.save(function(err, response)
		{
			demand(err).not.exist();
			Model.get(instance.key, function(err, retrieved)
			{
				demand(err).not.exist();
				retrieved.fetch_avatar(function(err, imagedata)
				{
					demand(err).not.exist();
					demand(imagedata).not.exist();
					done();
				});
			});
		});
	});

	it('can remove a document from the db', function(done)
	{
		instance.destroy(function(err, deleted)
		{
			demand(err).not.exist();
			deleted.must.exist();
			instance.destroyed.must.be.true();
			done();
		});
	});

	it('can remove documents in batches', function(done)
	{
		var obj2 = new Model();
		obj2.key = '4';
		obj2.name = 'two';
		obj2.save(function(err, response)
		{
			Model.get('2', function(err, obj)
			{
				demand(err).not.exist();
				obj.must.be.an.object();

				var itemlist = [obj, obj2.key];
				Model.destroyMany(itemlist, function(err, response)
				{
					demand(err).not.exist();
					// TODO examine response more carefully
					done();
				});
			});
		});
	});

	it('destroyMany() does nothing when given empty input', function(done)
	{
		Model.destroyMany(null, function(err)
		{
			demand(err).not.exist();
			done();
		});
	});

	it('destroy responds with an error when passed an object without an id', function(done)
	{
		var obj = new Model();
		obj.destroy(function(err, destroyed)
		{
			err.must.be.an.object();
			err.message.must.equal('cannot destroy object without an id');
			done();
		});
	});

	it('destroy responds with an error when passed an object that has already been destroyed', function(done)
	{
		var obj = new Model();
		obj.key = 'foozle';
		obj.destroyed = true;
		obj.destroy(function(err, destroyed)
		{
			err.must.be.an.object();
			err.message.must.equal('object already destroyed');
			done();
		});
	});

	it('removes attachments when removing an object', function(done)
	{
		var obj = new Model();
		obj.key = 'cats';
		obj.frogs = 'Cats do not eat frogs.';
		obj.name = 'all about cats';

		obj.save(function(err, reply)
		{
			demand(err).not.exist();
			reply.must.equal('OK');

			obj.destroy(function(err, destroyed)
			{
				demand(err).not.exist();
				done();
			});
		});
	});

	it('shutdown() closes the connection', function(done)
	{
		Model.adapter.shutdown(function()
		{
			demand(Model.adapter.connection).be.null();
			done();
		});
	});

	it('shutdown() can be called without a connection object', function(done)
	{
		Model.adapter.once('ready', function()
		{
			done();
		});
		Model.adapter.shutdown(function()
		{
			demand(Model.adapter.connection).be.null();
			Model.adapter.connect(); // reconnect for the next test
		});
	});

	after(function(done)
	{
		Rethink.dbDrop('test')
		.run(Model.adapter.connection, function(err)
		{
			done();
		});
	});
});
