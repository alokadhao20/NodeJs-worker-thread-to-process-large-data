var MongoClient = require('mongodb').MongoClient,
    format = require('util').format,
    async = require('async');
var bulk;
process.on("message", function(userArray) {
    MongoClient.connect('mongodb://127.0.0.1:27017/clickDB', function(err, db) {
        if (err) {
            throw err;
        } else {
            this.db = db;
            var collection = db.collection('clicks');
            bulk = collection.initializeUnorderedBulkOp();
            async.map(userArray, getAnddeleteRecursiveData.bind(null, this.db), function(err, result) {
                try {
                    bulk.execute(function(err, result) {
                        if (err) {
                            process.send(err);
                        } else {
                            process.send(result);
                        }
                    });
                } catch (err) {
                    process.send("No Operation in bulk");
                }
            });
        }
    });
});

var getAnddeleteRecursiveData = function(db, userID, callback) {
    var self = this;
    var collection = self.db.collection('clicks');
    async.waterfall([
        function(callback) {
            findDocuments(self.db, userID, function(err, data) {
                if (err) {
                    callback(err, null);
                } else {
                    callback(null, self.db, data);
                }
            });
        },
        function(db, data, callback) {
            var collection = db.collection('clicks');
            deleteDocuments(db, data, function(err, result) {
                callback(null, result);
            })
        }
    ], function(err, result) {
        callback(null, 'done');
    });

}

var deleteDocuments = function(db, eachUserData, callback) {
    var collection = db.collection('clicks');
    var index = 0;
    if (eachUserData.length > 1) {
        for (var k = 1; k < eachUserData.length; k++) {
            if (eachUserData[index].URL === eachUserData[k].URL) {
                bulk.find({
                    _id: eachUserData[k]._id
                }).remove();
            }
            index++;
            if (k == eachUserData.length - 1) {
                callback(null, 'done');
            }
        }
    } else {
        callback(null, 'done');
    }
}

var findDocuments = function(db, userID, callback) {
    var self = this;
    var collection = db.collection('clicks');
    collection.find({
        ID: userID
    }).toArray(function(err, docs) {
        if (err) {
            callback(err, null);
        } else {
            callback(null, docs);
        }
    });
}