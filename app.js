var express = require('express'),
    http = require('http'),
    path = require('path'),
    async = require('async'),
    cp = require("child_process");
var app = express();
var url = require("url");
app.set('port', process.env.PORT || 5556);
app.use(express.favicon());
app.use(express.logger('dev'));
app.use(express.bodyParser());
app.use(express.methodOverride());
app.use(app.router);
app.use(express.static(path.join(__dirname, 'public')));
app.use('/js', express.static(__dirname + '/public/js'));
app.use(express.static(__dirname + '/public/js'));
var noOfChildProcess = 3;
var MongoClient = require('mongodb').MongoClient,
    format = require('util').format;

MongoClient.connect('mongodb://127.0.0.1:27017/clickDB', function(err, db) {
    if (err) {
        throw err;
    } else {
        dbOnconnect(db, function(err, result) {

        });
    }
});

var dbOnconnect = function(db, callback) {
    this.db = db;
    async.waterfall([
        function(callback) {
            insertDocuments(db, function(err, result) {
                if (err) {
                    callback(err);
                } else {
                    callback(null, db, result);
                }
            });
        },
        function(db, result, callback) {
            getTotal(db, function(err, TotalUsersArray) {
                if (err) {
                    callback(err);
                } else {
                	console.log("Total users are ",TotalUsersArray);
                    callback(null, db, TotalUsersArray);
                }
            });
        },
        function(db, userArray, callback) {
            var spliceArray = splitArray(userArray, noOfChildProcess);
            console.log("spliceArray are ",spliceArray);
            async.each(spliceArray, ProcessSpicedArray, function(err) {
                callback(null, 'Done');
            });
        }
    ], function(err, result) {
        console.log("final result is " + result);
    });
}

var ProcessSpicedArray = function(spicedArray, doneCallback) {
    var worker = cp.fork("./worker");
    worker.on("message", function(data) {
        try {
            process.kill(this.pid)
            doneCallback(null);
        } catch (ex) {
            doneCallback(ex);
        }
    });
    worker.send(spicedArray);
};

var splitArray = function(userArray, n) {
    var len = userArray.length,
        out = [],
        i = 0;
    while (i < len) {
        var size = Math.ceil((len - i) / n--);
        out.push(userArray.slice(i, i += size));
    }
    return out;
}

var getTotal = function(db, callback) {
    var clicks = db.collection('clicks');
    clicks.distinct("ID", function(err, count) {
        callback(err, count);
    });

}

var insertDocuments = function(db, callback) {
    var clicks = db.collection('clicks');
    clicks.insert([{
        URL: 'https://www.linkedin.com/company/rapidera-technologies',
        ID: 100,
        Date: new Date()
    }, {
        URL: 'https://www.linkedin.com/company/rapidera-technologies',
        ID: 200,
        Date: new Date()
    }, {
        URL: 'https://www.linkedin.com/company/rapidera-technologies',
        ID: 300,
        Date: new Date()
    }], function(err, result) {
        console.log("Document Inserted-" + result);
        callback(err, result);
    });
}