'use strict';

/*
定期清理，可以使用node-schedule实现
清理的逻辑：删掉与当前时间差2小时以上的记录
ctrl-C退出的时候，关闭数据库+取消定时任务
数据库表结构：20170125版本-16个字段

*/



var P2PSpider = require('../lib');
var schedule = require('node-schedule');
var sqlite3 = require('sqlite3').verbose();
var db = new sqlite3.Database(':memory:');
var timerCnt = 0;
var wDiskMin = 5;
var cDbaseHour = 12;
var keepHour = 12;

db.serialize(function() {
    db.run("ATTACH DATABASE './magnetdb/magbase.sqlite3' AS dbDisk");
    db.run("CREATE TABLE IF NOT EXISTS dbDisk.magtabDisk (cnt INTEGER, lastseestr TEXT, filename TEXT, fileext TEXT, likeext INTEGER, maxsize INTEGER, pub TEXT, puburl TEXT, mag TEXT, name TEXT,totalsize INTEGER, filenum INTEGER, ip TEXT, port INTEGER, modified INTEGER, lastsee INTEGER)");
    db.run("CREATE TABLE main.magtabMem AS SELECT * FROM dbDisk.magtabDisk");
    db.run("CREATE INDEX IF NOT EXISTS magtabMem_mag ON magtabMem(mag)");
    db.run("UPDATE magtabMem SET modified=0");
});

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
    db.get("SELECT COUNT(*) AS count FROM magtabMem WHERE mag = ?", magnet,
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
                db.run("UPDATE magtabMem SET cnt=cnt+1, modified=modified+1, lastseestr=?, lastsee=?, ip=?, port=? WHERE mag=?", fetchTimeStr, fetchTime, addr0, port0, magnet);
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
    db.run("INSERT INTO magtabMem VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)",1,fetchTimeStr,filename,fileext,likeext,maxSize,pub,pubUrl,magnet,torName,totalSize,fileNum,addr0,port0,10000,fetchTime);
    console.log(magnet);
});

//每10分钟写一次硬盘,每2小时清理一次数据库,删除2小时之前的数据,因此保留的数据在2~4小时之间
var job = schedule.scheduleJob('*/'+wDiskMin+' * * * *', function(){
    db.all("SELECT * FROM magtabMem WHERE modified!=0", function(err, rows){
        if(!err){
            for(let row of rows){
                db.run("UPDATE magtabMem SET modified=0 WHERE mag=?", row.mag);
                if(row.modified<10000){
                    db.run("UPDATE magtabDisk SET cnt=?, lastseestr=?, lastsee=?, ip=?, port=?, modified=0 WHERE mag=?", row.cnt, row.lastseestr, row.lastsee, row.ip, row.port, row.mag);
                } else {
                    db.run("INSERT INTO magtabDisk VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)", row.cnt, row.lastseestr, row.filename, row.fileext, row.likeext, row.maxsize, row.pub, row.puburl, row.mag, row.name, row.totalsize, row.filenum, row.ip, row.port, 0, row.lastsee);
                }
            }
        }
    });
    timerCnt+=1;
    if(timerCnt >= cDbaseHour*60/wDiskMin){
        var curDate = new Date();
        var cleanTime = curDate.getTime()-keepHour*3600*1000;
        db.run("DELETE FROM magtabMem WHERE lastsee<?",cleanTime);
        db.run("DELETE FROM magtabDisk WHERE lastsee<?",cleanTime);
        timerCnt = 0;
    }
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