## Introduction
redadder is a simple redis ORM for node.js. It implements simple access to object properties saved in redis, supporting all the redis data-types.


## Features
- simple schemas
  - attributes
  - indexes
  - object relations
  - basic inheritance
- queries
- custom events


## Example
```javascript
var red = require('redadder')(6379); // the port where redis listens
var User = red('user', // the name of the class
  {
    attributes: {
      name: 'string',
      email: 'string',
      sortedSet: 'zset', // neat, but not that useful
      age: 'string', // remember that in redis numbers are strings
      friends: 'users', // user pluralized -> collection of users
      bestFriend: 'user',
      fb: 'string' // for facebook login, or whathaveyou
    },
    indexes: ['fb'] // lets you use User.fetchBy(fbid, 'fb')
  }
);

User.bind('destroy', function(user) {
  return red.query(User).test('friends', 'includes', user).exec().then(function(users) {
    for(var i = 0, promises = []; i < users.length; i++) {
      users[i].get('friends').remove(user);
      promises.push(users[i].save());
    }
    return Q.all(promises);
  });
});

var mark = new User();
mark.save({
  name: 'mark markson',
  email: 'm@r.k',
  fb: 12
}).then(function(mark) { console.log(mark.id); }); // redadder uses Q promises

var steve = new User().save({
  name: 'steve',
  email: 'i don\'t have one QQ',
  friends: [mark]
});

steve.bind('change:email', function(user) {
  console.log(user.get('name') + ' changed his email to ' + user.get('email'));
});
steve.set('email', 'steveisacooldudewithasweetnewemailaddress@hotmail.com').save();


var Admin = red('admin', User, { // inheritance
  attributes: {},
  indexes: []
});
```


## Future
- binding events to classes (ie, User.bind('soforth'); (done!)
- formal collections (ie, so we can do things like user.get('friends').remove(steve); (done-ish!)
- implement natural functions for zsets, sets, etc. & allow for zsets with object values etc.
- computed properties
