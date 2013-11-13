// UTIL
var merge = function(a, b) { for(var i in b) if(!a[i]) a[i] = b[i]; }
if(!Array.prototype.toJSON) Array.prototype.toJSON = function() {
	for(var i = 0, l = this.length, r = []; i < l; i++) r.push(this[i] && this[i].toJSON ? this[i].toJSON() : this[i]);
	return r;
}


// Prepare redis && Q-ify it
var commands = require('./commands'),
	redis = require('redis'),
	Q = require('q');

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
	})(redis.RedisClient.prototype[command]);
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
})(redis.Multi.prototype.exec);


// GLOBALS
var dataTypes = ['string', 'number', 'set', 'zset', 'hash', 'list'],
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
		if(!this._bindings) return Q();
		e = e.split(' ');
		for(var i = 0, name; name = e[i]; i++) {
			if(!this._bindings[name]) continue;
			else for(var j = 0, fn; fn = this._bindings[name][j]; j++) promises.push(fn.apply(fn._context, args));
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
	
	var Collection = function(attr, object) {
		var klass = classes[en.singularize(object.schema.attributes[attr])],
			members = this.members = [];
		
		this.fetch = function() {
			var that = this;
			return client.smembersQ(object.id + attr).then(function(ids) {
				return klass.fetch(ids);
			}, function() { return []; }).then(function(objects) {
				that.set(objects);
				object.changed[attr] = false;
				return that;
			});
		}
		this.add = function(obj) {
			var that = this;
			if(!(obj instanceof klass)) obj = new klass(obj.id || obj);
			return client.saddQ(object.id + attr, obj.id).then(function(added) {
				if(added) members.push(obj);
				return that;
			});
		}
		this.remove = function(obj) {
			var that = this;
			if(!(obj instanceof klass)) obj = new klass(obj.id || obj);
			return client.sremQ(object.id + attr, obj.id).then(function(removed) {
				if(removed) for(var i = 0; i < members.length; i++) if(members[i].id == obj.id) members.splice(i, 1), --i;
				return that;
			});
		}
		this.set = function(objects) {
			for(var i = 0, ids = []; i < members.length; i++) ids.push(members[i].id);
			for(var i = 0, change = false; i < objects.length; i++) change = !~ids.indexOf(objects[i].id) || change;
			if(!change) return false;
			members.length = 0;
			if(objects instanceof Array) for(var i = 0; i < objects.length; i++) members.push(objects[i]);
			return true;
		}
		this.toJSON = function(done) {
			if(!done) done = {};
			for(var i = 0, res = []; i < members.length; i++)
				res.push(members[i].toJSON ? members[i].toJSON(done) : members[i]);
			return res;
		}
	}
	
	var add = function(prefix) {
		var red = function(name) {
			var type = prefix + name,
				supers = Array.prototype.slice.call(arguments, 1),
				schema = supers.pop();
			
			if(!schema.attributes) schema.attributes = {};
			if(!schema.indexes) schema.indexes = [];

			for(var i = 0; i < supers.length; i++) {
				merge(schema.attributes, supers[i].schema.attributes);
				for(var j = 0, attr; attr = supers[i].schema.indexes[j]; j++)
					if(!~schema.indexes.indexOf(attr)) schema.indexes.push(attr);
			}
			
			var parse = function(attr, raw) {
				switch(schema.attributes[attr]) {
					case 'string': return raw instanceof Error ? null : raw;
					case 'number': return raw instanceof Error ? null : parseFloat(raw);
					case 'set': case 'list': return raw instanceof Error ? [] : raw;
					case 'zset': case 'hash': return arrayToHash(raw instanceof Error ? {} : raw);
					default: return objectify(attr, raw instanceof Error ? collect(attr) ? [] : null : raw);
				}
			}
			var objectify = function(attr, raw) {
				if(!raw) return null;
				var c;
				if(c = classes[schema.attributes[attr]]) raw = raw instanceof c ? raw : new c(raw.id || raw);
				else if(c = collect[attr]) for(var i = 0; i < raw.length; i++) if(!(raw[i] instanceof c))
						raw[i] = raw[i] && new c(raw[i].id || raw[i]);
				return raw;
			}
			var collect = function(attr) {
				return !classes[schema.attributes[attr]] && classes[en.singularize(schema.attributes[attr])];
			}
			
			var klass = function(id) {
				if(id) this.id = id && id.id || id;
				this.attributes = {};
				this.changed = {};
				this.indexes = {};
				this.schema = klass.schema;

				for(var attr in schema.attributes) if(collect(attr)) this.attributes[attr] = new Collection(attr, this);

				var check = function(attr, val) {
					if(!schema.attributes[attr]) return false;
					else if(arguments.length == 1) return true;
					else switch(schema.attributes[attr]) {
					case 'string':
						return typeof val === 'string';
					case 'number':
						return typeof val === 'number';
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
					case 'string': case 'number':
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
						if(classes[schema.attributes[attr]]) multi.get(this.id + attr);
						else if(collect(attr)) multi.lrange(this.id + attr, 0, -1);
					}
				}
				this.parse = function(values) {
					var i = 0;
					for(var attr in schema.attributes) {
						if(collect(attr)) this.attributes[attr].set(parse(attr, values[i]));
						else this.attributes[attr] = parse(attr, values[i]);
						this.changed[attr] = false;
						i++;
					}
					for(var j = 0, attr; attr = schema.indexes[j]; j++) this.indexes[attr] = this.attributes[attr];
					return i;
				}
				this.fetch = function(opts) {
					if(!opts) opts = {};
					var that = this,
						multi = client.multi();
					this.getcommand(multi);
					return multi.execQ().then(function(raw) {
						that.parse(raw);
						var promises = [];
						if(opts.depth > 0) for(var attr in that.attributes)
							that.attributes[attr].fetch && promises.push(that.attributes[attr].fetch(depth - 1));
						return Q.all(promises).then(function() {
							return that.trigger('fetch', that);
						}).then(function() { return that; });
					});
				}
				this.get = function(attr, fetch) {
					if(!check(attr)) return false;
					if(fetch && this.changed[attr] === undefined) {
						var multi = client.multi();
						this.getcommand(multi, [attr]);
						return multi.execQ().then(function(raw) {
							this.changed[attr] = false;
							if(collect(attr)) this.attributes[attr].set(parse(attr, raw));
							else this.attributes[attr] = parse(attr, raw);
							return this.attributes[attr];
						});
					}
					return this.attributes[attr];
				}
				this.set = function(attr, value, opts) {
					if(!opts) opts = {};
					if(check(attr, value)) {
						if(collect(attr)) this.changed[attr] = this.attributes[attr].set(objectify(attr, value));
						else if(classes[schema.attributes[attr]] && this.attributes[attr].id != objectify(attr, value).id) {
							this.attributes[attr] = objectify(attr, value);
							this.changed[attr] = true;
						} else if(this.attributes[attr] != value) {
							this.attributes[attr] = value;
							this.changed[attr] = true;
						}
						if(!opts.quiet && this.changed[attr]) {
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
						case 'string': case 'number':
							attrs[attr] != null && multi.set(this.id + attr, attrs[attr]);
							break;
						case 'set':
							attrs[attr] && attrs[attr].length && multi.sadd(this.id + attr, attrs[attr]);
							break;
						case 'zset':
							attrs[attr] && Object.keys(attrs[attr]).length && multi.zadd(this.id + attr, hashToArray(attrs[attr]));
							break;
						case 'hash':
							attrs[attr] && Object.keys(attrs[attr]).length && multi.hmset(this.id + attr, hashToArray(attrs[attr]));
							break;
						case 'list':
							attrs[attr] && attrs[attr].length && multi.rpush(this.id + attr, attrs[attr]);
							break;
						default:
							if(classes[schema.attributes[attr]])
								attrs[attr] && multi.set(this.id + attr, attrs[attr].id || attrs[attr]);
							else if(collect(attr)) {
								for(var i = 0, ids = []; i < attrs[attr].length; i++)
									ids.push(attrs[attr][i] && attrs[attr][i].id);
								ids.length && multi.rpush(this.id + attr, ids);
							}
						}
					}
					return multi.execQ();
				}
				this.save = function(attrs, opts) {
					if(attrs) for(var attr in attrs) this.set(attr, attrs[attr], { quiet: true });
					else {
						attrs = {};
						for(var attr in schema.attributes) if(this.changed[attr]) attrs[attr] = this.attributes[attr];
					}
					
					var that = this,
						multi = client.multi();
					
					for(var i = 0, attr; attr = schema.indexes[i]; i++) multi.hget(type + attr, attrs[attr]);
					return multi.execQ().then(function(values) {
						for(var i = 0; i < values.length; i++) if(values[i]) throw new Error('Index already in use.');
						// If our indexes are free, then lets save.
						if(that.id) return that.update(attrs).then(function() {
							// Change indexes
							for(var i = 0, attr; attr = schema.indexes[i]; i++) if(that.changed[attr]) {
								client.hdel(type + attr, that.indexes[attr]);
								client.hset(type + attr, that.indexes[attr] = attrs[attr], that.id);
							}
							for(var attr in attrs) that.changed[attr] = false;
							return that.trigger('save', that).then(function() { return that; });
						});
						else return client.incrQ('id').then(function(id) {
							// Add to type set and types hash
							that.id = id;
							client.sadd(type, id);
							client.hset('types', id, type);
							return that.update(attrs).then(function() {
								// Create indexes
								for(var i = 0, attr; attr = schema.indexes[i]; i++)
									client.hset(type + attr, that.indexes[attr] = attrs[attr], id);
								for(var attr in attrs) that.changed[attr] = false;
								return klass.trigger('create', that).then(function() { return that; });
							});
						});
					});
				}
				this.destroy = function() {
					var that = this;
					return klass.trigger('destroy', this).then(function() {
						return that.trigger('destroy', that);
					}).then(function() {
						for(var i = 0, attr; attr = schema.indexes[i]; i++)
							client.hdel(type + attr, that.indexes[attr]);
						for(var attr in schema.attributes) client.del(that.id + attr);
						client.srem(type, that.id);
						client.hdel('types', that.id);
						return true;
					});
				}
				this.toJSON = function(done) {
					if(!done) done = {};
					if(done[this.id]) return { id: this.id };
					else done[this.id] = true;
					var clone = { id: this.id };
					for(var attr in this.attributes) if(this.attributes[attr])
						clone[attr] = this.attributes[attr].toJSON ? this.attributes[attr].toJSON(done) : this.attributes[attr];
					return clone;
				}
			}

			merge(klass.prototype, eventful);
			merge(klass, eventful);
			
			classes[type] = klass;
			klass.add = add(type);
			klass.schema = schema;
			
			/* Public */
			klass.ids = function() { return client.smembersQ(type); }
			klass.fetch = function(ids) {
				var singular = false,
					objects = [];
				
				if(arguments.length == 0) return klass.ids(function(ids) { return klass.fetch(ids); });
				else if(!ids) return null;
				else if(ids.length == 0) return [];
				else if(!(ids instanceof Array)) ids = [ids], singular = true;
				
				for(var i = 0; i < ids.length; i++) ids[i] = parseInt(ids[i].id || ids[i]);
				return client.hmgetQ('types', ids).then(function(types) {
					for(var i = 0, multi = client.multi(); i < ids.length; i++) {
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
				
				return client.hmgetQ(type + index, values).then(function(ids) { return klass.fetch(singular ? ids[0] : ids); });
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
				test: function(ids, attr, test, against) {
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
				sort: function(ids, attr) {
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
				skip: function(ids, skip) { return ids.slice(skip); },
				limit: function(ids, limit) { return ids.slice(0, limit); }
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
				return result.then(function(ids) { return klass.fetch(ids); });
			}
			return deferred.promise;
		}
		return red;
	}
	return add('');
}