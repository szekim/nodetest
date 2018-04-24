
var express = require('express');
var bodyParser = require('body-parser');
var mongoose = require('mongoose');
var http = require('http');

var Schema = mongoose.Schema;
const PORT = process.env.PORT || 5000

const dbURI =
  "mongodb://szekim:kim12345678@kimcluster-shard-00-00-w4ocf.mongodb.net:27017,kimcluster-shard-00-01-w4ocf.mongodb.net:27017,kimcluster-shard-00-02-w4ocf.mongodb.net:27017/test?ssl=true&replicaSet=KimCluster-shard-0&authSource=admin";

const options = {
  reconnectTries: Number.MAX_VALUE,
  poolSize: 10
};

mongoose.connect(dbURI, options).then(
  () => {
    console.log("Database connection established!");
  },
  err => {
    console.log("Error connecting Database instance due to: ", err);
  }
);

var dataSchema = mongoose.Schema({
      myKey: String,
      value: String,
      timeStamp:{ type: Date, default: Date.now()}
});
var data = mongoose.model('Data', dataSchema)


var app = express();
app.use(bodyParser.json());


http.createServer(app).listen(PORT);
console.log("The server is start listening on port 5000");

app.post('/object', function (req, res) {
    var key = Object.keys(req.body)[0];
    var value = req.body[key];
    var dateNow = new Date()
   var newData = new data({ myKey: key, value: value, timeStamp: dateNow });
    var response = {"key": key, "value": value, "timestamp": (dateNow.getTime()/1000 | 0) } 
    newData.save(function(err){
       if (err) throw err;
            console.log("saved")
           res.send(response)
            res.end()
    });
}); 

app.get("/object/:key", function(req, res){
    if (Object.keys(req.query).length === 0){
        data.find({myKey:req.params.key}, function(err,result){
            if (err) throw err;
            if (result.length > 0){
                lastestResult = result[result.length -1 ]
            var response = { "value": lastestResult.value }
            res.send(response)
            res.end()
            }
            else{
                res.send("Invalid Key")
                res.end()
            }
            
        })
    }
    else {
        data.find({myKey:req.params.key}, function(err,result){
            var queryDate = new Date(parseInt(req.query.timestamp * 1000))
            var queryResult = findClosestTime(result, queryDate)
            if (!queryResult.value) res.send(queryResult)
            else res.send({value: queryResult.value})
            res.end()
        })
    }
});


function findClosestTime(keyData, queryDate) {
    var before = [];
    var max = keyData.length;
    for(var i = 0; i < max; i++) {
        var diff = ((keyData[i].timeStamp/1000) - (queryDate/1000));
        if(diff < 1) {
            before.push({diff: diff, index: i, data: keyData[i]});
        }
    }
    before.sort(function(a, b) {
        if(a.diff > b.diff) {
            return -1;
        }
        if(a.diff < b.diff) {
            return 1;
        }
        return 0;
    });
    if (before.length == 0) {
        return "Invalid timestamp"
    }
    else {
        return before[0].data
    }
}
