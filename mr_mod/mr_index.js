'use strict';

/*
定期清理，可以使用node-schedule实现
清理的逻辑：把数据库看lastsee排序，从第1万条后面开始删，删掉那些与当前时间差3天以上的
ctrl-C退出的时候，关闭数据库+取消定时任务
数据库表结构：mag,name,totalsize,filenum,filename,maxsize,lastsee,cnt

*/



var P2PSpider = require('../lib');
var schedule = require('node-schedule');
var sqlite3 = require('sqlite3').verbose();
var db = new sqlite3.Database('./magnetdb/magbase.sqlite3');
db.run("CREATE TABLE IF NOT EXISTS magtab (cnt INTEGER, lastseestr TEXT, filename TEXT, fileext TEXT, likeext INTEGER, maxsize INTEGER, pub TEXT, puburl TEXT, mag TEXT, name TEXT,totalsize INTEGER, filenum INTEGER, ip TEXT, port INTEGER, lastsee INTEGER)", [],
function(err){db.run("CREATE INDEX IF NOT EXISTS magtab_mag ON magtab(mag)");} );


var p2p = P2PSpider({
    nodesMaxSize: 200,   // be careful
    maxConnections: 400, // be careful 默认400，根据学校情况改成80
    timeout: 5000
});

p2p.ignore(function (infohash, rinfo, callback) {
    // false => always to download the metadata even though the metadata is exists.
    //var theInfohashIsExistsInDatabase = false;
    //callback(theInfohashIsExistsInDatabase);
    //console.log("Hash found!");
    var magnet = 'magnet:?xt=urn:btih:'+infohash;
    db.get("SELECT COUNT(*) AS count FROM magtab WHERE mag = ?", magnet,
        function(err, row){
            var date = new Date();
            var fetchTime = date.getTime();
            var fetchTimeStr = date.toLocaleString();
            var addr0 = rinfo.address;
            var port0 = rinfo.port;
            var existMag=row.count;
            if(existMag==0){
                callback(false);
                //console.log("New hash!");
            } else{
                callback(true);
                db.run("UPDATE magtab SET cnt=cnt+1, lastseestr=?, lastsee=?, ip=?, port=? WHERE mag=?", fetchTimeStr, fetchTime, addr0, port0, magnet);
                //console.log("Repeated hash!");
            }
        }
    );
});

p2p.on('metadata', function (metadata) {
    var magnet = metadata.magnet;
    var date = new Date();
    var fetchTime = date.getTime();
    var fetchTimeStr = date.toLocaleString();
    var addr0 = metadata.address;
    var port0 = metadata.port;
    //(metadata.info["name.utf-3"]||metadata.info["name.utf-8"]).toString()
    var torName = (metadata.info["name.utf-8"] || metadata.info["name"]).toString();
    var pub = (metadata.info["publisher.utf-8"] || metadata.info["publisher"] || "").toString();
    var pubUrl = (metadata.info["publisher-url.utf-8"] || metadata.info["publisher-url"] || "").toString();
    var totalSize = metadata.info.length;
    var maxSize = totalSize;
    var fileNum = 1;
    var filename = torName;
    if("files" in metadata.info){
        totalSize = 0;
        maxSize = 0;
        fileNum = metadata.info.files.length;
        var i = 0;
        var filepath_i = '';
        for (i in metadata.info.files){
            totalSize+=metadata.info.files[i].length;
            if(metadata.info.files[i].length>maxSize){
                maxSize = metadata.info.files[i].length;
                filepath_i = (metadata.info.files[i]["path.utf-8"] || metadata.info.files[i]["path"]);
                filename = filepath_i[filepath_i.length-1].toString();
            }
        }
    }
    totalSize = Math.ceil(totalSize/(1024*1024));
    maxSize = Math.ceil(maxSize/(1024*1024));
    var fileext = filename.split('.').pop().toLowerCase();
    var likeext = 0;
    if(fileext.match(/(avi|mp4|mkv|wmv|vob|mpg|rmvb|m4v|m2ts|flv|mov|rm|3gp|mpeg|divx)$/)){
        likeext = 1;
    }
    db.run("INSERT INTO magtab VALUES (1,?,?,?,?,?,?,?,?,?,?,?,?,?,?)",fetchTimeStr,filename,fileext,likeext,maxSize,pub,pubUrl,magnet,torName,totalSize,fileNum,addr0,port0,fetchTime);
    console.log(magnet);
});

//每天清理10点17分清理，保留1天半的数据
var job = schedule.scheduleJob('23 17 10 * * *', function(){
    var curDate = new Date();
    var cleanTime = curDate.getTime()-86400*1000;
    db.run("DELETE FROM magtab WHERE lastsee<?",cleanTime);
    console.log('Clean Done: ' + curDate.toLocaleString());
});

//猜测该函数是用来在Ctrl-C的时候关闭数据库的
process.on('SIGINT', function() {
    db.close(function(err) {
        console.log("DB closed!");
    });
    job.cancel();
    process.exit();
});

//默认端口6881
p2p.listen(6881, '0.0.0.0');