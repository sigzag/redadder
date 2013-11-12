// UTIL
var merge = function(a, b) { for(var i in b) if(!a[i]) a[i] = b[i]; }
if(!Array.prototype.toJSON) Array.prototype.toJSON = function() {s
	for(var i = 0, l = this.length, r = []; i < l; i++) r.push(this[i] && this[i].toJSON ? this[i].toJSON() : this[i]);
	return r;
}


// Prepare redis && Q-ify it
var commands = require('./commands');
var redis = require('redis');
for(var i = 0, command; command = commands[i]; i++) {
	redis.RedisClient.prototype[command + 'Q'] = (function(fn) {
		return function() {
			var args = Array.prototype.slice.call(arguments),
				deferred = Q.defer();
			args.push(function(err, res) {
				if(err) deferred.reject(err);
				else deferred.resolve(res);
			});
			fn.apply(this, args);
			return deferred.promise;
		}
	})(mongo.Collection.prototype[command]);
}
redis.Multi.prototype.execQ = (function(fn) {
	return function() {
		var args = Array.prototype.slice.call(arguments),
			deferred = Q.defer();
		args.push(function(err, res) {
			if(err) deferred.reject(err);
			else deferred.resolve(res);
		});
		fn.apply(this, args);
		return deferred.promise;
	}
})(redis.Multi.prototype.execQ);


// GLOBALS
var dataTypes = ['string', 'set', 'zset', 'hash', 'list'],
	classes = {},
	lingo = require('lingo'), en = lingo.en;


// Bind events like: create, destroy, fetch, change, etc.
var eventful = {
	bind: function(e, fn, context) {
		if(!this._bindings) this._bindings = {};
		fn._context = context;
		e = e.split(' ');
		for(var i = 0, name; name = e[i]; i++) {
			if(!this._bindings[name]) this._bindings[name] = [];
			this._bindings[name].push(fn);
			return this;
		}
	},
	unbind: function(e, fn) {
		if(!this._bindings) return;
		e = e.split(' ');
		for(var i = 0, name; name = e[i]; i++) {
			if(!this._bindings[name]) continue;
			else if(fn) ~this._bindings[name].indexOf(fn) && this._bindings[name].splice(this._bindings[name].indexOf(fn), 1);
			else this.bindings[name] = undefined;
		}
		return this;
	},
	trigger: function(e) {
		var args = Array.prototype.slice.call(arguments, 1),
			promises = [];
		if(!this._bindings) return;
		e = e.split(' ');
		for(var i = 0, name; name = e[i]; i++) {
			if(!this._bindings[name]) continue;
			else for(var j = 0; j < this._bindings[name].length; i++) promises.push(this._bindings[name][j].apply(fn._context, args));
		}
		return Q.all(promises);
	}
}
merge(eventful, { on: eventful.bind, off: eventful.unbind });


// PARSE UTIL
var hashToArray = function(hash) {
	if(!hash) hash = {};
	var keys = Object.keys(hash);
	for(var i = 0, array = []; i < keys.length; i++) array.push(keys[i], hash[keys[i]]);
	return array;
}
var arrayToHash = function(array) {
	if(!array) array = [];
	for(var i = 0, hash = {}; i < array.length; i++) hash[array[i]] = array[++i];
	return hash;
}
var fetchCollection = function(opts) {
	for(var i = 0, promises = []; i < this.length; i++) promises.push(this[i].fetch(opts));
	return Q.all(promises);
}


// red.query().test([queryTest], blah, blah, blah)
var queryTests = {
	'^=': function(a, b) { return a.slice(0, b.length) == b; },
	'$=': function(a, b) { return a.slice(-b.length) == b; },
	'=': function(a, b) { return val == against; },
	'>': function(a, b) { return parseFloat(a) > parseFloat(b); },
	'>=': function(a, b) { return parseFloat(a) >= parseFloat(b); },
	'<': function(a, b) { return parseFloat(a) < parseFloat(b); },
	'<=': function(a, b) { return parseFloat(a) <= parseFloat(b); },
	'includes': function(a, b) { return !!~a.indexOf(b); },
	'in': function(a, b) { return !!~b.indexOf(a); }
}


// The main event
module.exports = function(port) {
	var client = redis.createClient(port);
	client.on('error', function(err) { console.log('Connection down.'); client.closing = true; });
	
	var add = function(prefix) {
		var red = function(name) {
			var type = prefix + name,
				supers = Array.prototype.slice.call(arguments, 1),
				schema = supers.pop();

			for(var i = 0; i < supers.length; i++) {
				merge(schema.attributes, supers[i].schema.attributes);
				for(var j = 0, attr; attr = supers[i].schema.indexes[j]; j++)
					if(!~schema.indexes.indexOf(attr) schema.indexes.push(attr);
			}
			
			var parse = function(attr, raw) {
				switch(schema.attributes[attr]) {
					case 'string': case 'set': case 'list': return raw;
					case 'zset': case 'hash': return arrayToHash(raw);
					default: return objectify(attr, raw);
				}
			}
			var objectify = function(attr, raw) {
				if(!raw) return null;
				if(raw instanceof Array) {
					raw.fetch = fetchCollection;
					if(!raw.length) return raw;
				}
				var c;
				if(c = classes[schema.attributes[attr]]) raw = raw instanceof c ? raw : new c(raw.id || raw);
				else if(c = classes[en.singularize(schema.attributes[attr])])
					for(var i = 0; i < raw.length; i++) if(!(raw[i] instanceof c))
						raw[i] = raw[i] && new c(raw[i].id || raw[i]);
				return raw;
			}
			
			var klass = function(id) {
				if(id) this.id = id && id.id || id;
				this.attributes = {};
				this.changed = {};
				this.indexes = {};

				var check = function(attr, val) {
					if(!schema.attributes[attr]) return false;
					else if(arguments.length == 1) return true;
					else switch(schema.attributes[attr]) {
					case 'string':
						return typeof val === 'string';
					case 'set':
					case 'list':
						return val instanceof Array;
					case 'zset':
					case 'hash':
						return !!Object.keys(val);
					default:
						return true;
					}
				}
				
				// events: 'fetch', 'change', 'save', 'create', 'destroy'
				this.getcommand = function(multi, keys) {
					if(!keys) keys = Object.keys(schema.attributes);
					for(var i = 0, attr; attr = keys[i]; i++) switch(schema.attributes[attr]) {
					case 'string':
						multi.get(this.id + attr);
						break;
					case 'set':
						multi.smembers(this.id + attr);
						break;
					case 'zset':
						multi.zrange(this.id + attr, 0, -1, 'withscores');
						break;
					case 'hash':
						multi.hgetall(this.id + attr);
						break;
					case 'list':
						multi.lrange(this.id + attr, 0, -1);
						break;
					default:
						if(classes[this.schema.attributes[attr]])
							multi.get(this.id + attr);
						else if(classes[en.singularize(this.schema.attributes[attr])])
							multi.lrange(this.id + attr, 0, -1);
					}
				}
				this.parse = function(values) {
					var i = 0, c;
					for(var attr in schema.attributes) {
						this.attributes[attr] = parse(attr, values[i]);
						this.changed[attr] = false;
						i++;
					}
					for(var j = 0, attr; attr = schema.indexes[j]; j++) this.indexes[attr] = this.attributes[attr];
					return Q(i);
				}
				this.fetch = function(opts) {
					var that = this,
						multi = client.multi();
					getcommand(multi);
					multi.execQ().then(function(raw) {
						that.parse(raw);
						var promises = [];
						if(opts.depth > 0) for(var attr in that.attributes)
							that.attributes[attr].fetch && promises.push(that.attributes[attr].fetch(depth - 1));
						promises.push(function() { return that.trigger('fetch', that); });
						return Q.all(promises).then(function() { return that; });
					});
				}
				this.get = function(attr) {
					if(!check(attr)) return false;
					if(this.changed[attr] === undefined) {
						var multi = client.multi();
						this.getcommand(multi, [attr]);
						return multi.execQ().then(function(raw) {
							this.changed[attr] = false;
							return this.attributes[attr] = parse(raw);
						});
					}
					return this.attributes[attr];
				}
				this.set = function(attr, value, opts) {
					if(!opts) opts = {};
					if(check(attr, schema, value)) {
						this.attributes[attr] = objectify(value);
						this.changed[attr] = true;
						if(!opts.quiet) {
							this.trigger('change', this);
							this.trigger('change:' + attr, this);
						}
					}
				}
				this.update = function(attrs) {
					var multi = client.multi();
					for(var attr in attrs) if(this.changed[attr]) {
						multi.del(this.id + attr);
						switch(schema.attributes[attr]) {
						case 'string':
							multi.set(this.id + attr, attrs[attr]);
							break;
						case 'set':
							multi.sadd(this.id + attr, attrs[attr]);
							break;
						case 'zset':
							multi.zadd(this.id + attr, hashToArray(attrs[attr]));
							break;
						case 'hash':
							multi.hmset(this.id + attr, hashToArray(attrs[attr]));
							break;
						case 'list':
							multi.rpush(this.id + attr, attrs[attr]);
							break;
						default:
							if(classes[this.schema.attributes[attr]])
								multi.set(this.id + attr, attrs[attr] && attrs[attr].id);
							else if(classes[en.singularize(this.schema.attributes[attr])]) {
								for(var i = 0, ids = []; i < attrs[attr]; i++) ids.push(attrs[attr][i] && attrs[attr][i].id);
								multi.rpush(this.id + attr, ids);
							}
						}
					}
					return multi.execQ();
				}
				this.save = function(attrs) {
					if(attrs) for(var attr in attrs) this.set(attr, attr.value, { quiet: true });

					var that = this,
						multi = client.multi();
					
					for(var i = 0, attr; attr = schema.indexes[i]; i++) multi.hget(type + attr, attrs[attr]);
					multi.execQ().then(function(values) {
						for(var i = 0; i < values.length; i++) if(values[i]) throw new Error('Index already in use.');
						// If our indexes are free, then lets save.
						if(this.id) return that.update(attrs).then(function() {
							// Change indexes
							for(var i = 0, attr; attr = schema.indexes[i]; i++) if(that.changed[attr]) {
								client.hdel(type + attr, that.indexes[attr]);
								client.hset(type + attr, that.indexes[attr] = attrs[attr]);
							}
							for(var attr in attrs) that.changed[attr] = false;
							return that.trigger('save', that).then(function() { return that; });
						});
						else return client.incrQ('id').then(function(id) {
							// Add to type set and types hash
							client.sadd(type, this.id);
							client.hset('types', this.id);
							return that.update(attrs).then(function() {
								// Create indexes
								for(var i = 0, attr; attr = schema.indexes[i]; i++)
									client.hset(type + attr, that.indexes[attr] = attrs[attr]);
								for(var attr in attrs) that.changed[attr] = false;
								return that.trigger('create', that).then(function() { return that; });
							});
						});
					}):
				}
				this.destroy = function() {
					var that = this;
					return this.trigger('destroy', this).then(function() {
						for(var i = 0, attr; attr = schema.indexes[i]; i++)
							client.hdel(type + attr, that.indexes[attr]);
						for(var attr in schema.attributes) client.del(that.id + attr);
						client.srem(type, that.id);
						client.hdel('types', that.id);
					});
				}
				this.toJSON = function() {
					var clone = {};
					for(var attr in this.attributes) clone[attr] = this.attributes[attr].toJSON();
					return clone;
				}
			}

			merge(klass.prototype, eventful);
			
			classes[type] = klass;
			klass.add = add(type);
			klass.schema = schema;
			
			/* Public */
			klass.ids = function() { return client.smembersQ(type); }
			klass.fetch = function(ids) {
				var singular = false,
					objects = [];
				
				if(ids.length == 0) return [];
				else if(!ids) return klass.ids(function(ids) { return klass.fetch(ids); });
				else if(!(ids instanceof Array)) ids = [ids], singular = true;
				
				for(var i = 0; i < ids.length; i++) ids[i] = parseInt(ids[i].id || ids[i]);
				return client.hmgetQ('types', ids).then(function(types) {
					for(var i = 0, multi = client.multi(); i < ids.length; i++)
						objects[i] = new (klass[types[i]] || klass)(ids[i]);
						objects[i].getcommand(multi);
					}
					return multi.execQ().then(function(values) {
						for(var i = 0, j = 0; i < ids.length; i++) j += objects[i].parse(values.slice(j));
						return singular ? objects[0] : objects;
					});
				});
			}
			klass.fetchBy = function(values, index) {
				var singular = false;
				
				if(values.length == 0) return [];
				else if(!(values instanceof Array)) values = [values], singular = true;
				
				return client.hmgetQ(type + index, values).then(function(ids) { return klass.fetch(singluar ? ids[0] : ids); });
			}
			
			return klass;
		}
		red.classes = classes;
		red.client = client;
		red.query = function(klass) {
			var chain = [],
				deferred = new Q.defer(),
				ids = [], objects = [];
			var actions = {
				test = function(ids, attr, test, against) {
					if(!klass.schema.attributes[attr]) throw new Error('This attribute does not exist.');
					if(arguments.length < 4) throw new Error('Missing arguments');

					if(against && against.id) against = against.id;
					test = queryTest[test];
					for(var i = 0, q = []; i < ids.length; i++) q.push(ids[i] + attr);
					client.mgetQ(q).then(function(values) {
						for(var i = 0, res = []; i < ids.length; i++) if(test(values[i], against)) res.push(ids[i]);
						return res;
					});
				},
				sort = function(ids, attr) {
					if(!klass.schema.attributes[attr]) throw new Error('This attribute does not exist.');
					if(arguments.length < 2) throw new Error('Missing arguments');

					for(var i = 0, q = []; i < ids.length; i++) q.push(ids[i] + attr);
					client.mgetQ(q).then(function(values) {
						for(var i = 0, hash = {}; i < ids.length; i++) hash[values[i]] = ids[i];
						values.sort();
						for(var i = 0, res = []; i < ids.length; i++) res.push(hash[values[i]]);
						return res;
					});
				},
				skip = function(ids, skip) { return ids.slice(skip); },
				limit = function(ids, limit) { return ids.slice(0, limit); }
			}
			for(var action in actions) deferred.promise[action] = (function(action) {
				return function() {
					chain.unshift(Array.prototype.slice.call(arguments), actions[action]);
					return this;
				}
			})(action);
			deferred.promise.exec = function() {
				var result = client.smembersQ(klass.type);
				for(var i = 0, len = chain.length; i < len; i++) result = result.then(function(ids) {
					var args = chain.pop();
					args.unshift(ids);
					chain.pop().apply(null, args);
				});
				return result.then(function(ids) { return klass.get(ids); });
			}
			return deferred.promise;
		}
		return red;
	}
	return add('');
}