'use strict';

/*
定期清理，可以使用node-schedule实现
清理的逻辑：把数据库看lastsee排序，从第1万条后面开始删，删掉那些与当前时间差3天以上的
ctrl-C退出的时候，关闭数据库+取消定时任务
数据库表结构：mag,name,totalsize,filenum,filename,maxsize,lastsee,cnt

*/



var P2PSpider = require('../lib');
var schedule = require('node-schedule');
var mysql = require('mysql');
var cleanHours = 8;
var keepHours = 8;

var remCnt=3;
setInterval(function(){remCnt=101;}, 1000);

var db = mysql.createConnection({
    host: '127.0.0.1',
    user: 'root',
    password: '',
    database: 'magdb',
    charset: 'utf8'
});
db.connect();
db.query("CREATE TABLE IF NOT EXISTS `magtab` (`cnt` INT(11) NULL DEFAULT NULL,`lastseestr` CHAR(20) NULL DEFAULT NULL,`filename` VARCHAR(255) NULL DEFAULT NULL,`fileext` VARCHAR(150) NULL DEFAULT NULL,`likeext` INT(11) NULL DEFAULT NULL,`maxsize` INT(11) NULL DEFAULT NULL,`pub` VARCHAR(60) NULL DEFAULT NULL,`puburl` VARCHAR(255) NULL DEFAULT NULL,`mag` CHAR(60) NULL DEFAULT NULL,`name` VARCHAR(255) NULL DEFAULT NULL,`totalsize` INT(11) NULL DEFAULT NULL,`filenum` INT(11) NULL DEFAULT NULL,`ip` CHAR(15) NULL DEFAULT NULL,`port` INT(11) NULL DEFAULT NULL,`lastsee` BIGINT(20) NULL DEFAULT NULL) COLLATE='utf8_general_ci' ENGINE=MyISAM;");


var p2p = P2PSpider({
    nodesMaxSize: 8,   // be careful
    maxConnections: 32, // be careful 默认400，根据学校情况改成80
    timeout: 5000
});

p2p.ignore(function (infohash, rinfo, callback) {
    // false => always to download the metadata even though the metadata is exists.
    //var theInfohashIsExistsInDatabase = false;
    //callback(theInfohashIsExistsInDatabase);
    //console.log("Hash found!");
    if (remCnt>0) {
        var magnet = 'magnet:?xt=urn:btih:'+infohash;
        remCnt-=1;
        db.query("SELECT COUNT(*) AS count FROM magtab WHERE mag = ?", [ magnet ],
            function(err, rows, fields){
                var date = new Date();
                var fetchTime = date.getTime();
                var fetchTimeStr = date.toLocaleString('en-US',{timeZone:"Asia/Shanghai", hour12:false});
                var addr0 = rinfo.address;
                var port0 = rinfo.port;
                var existMag=rows[0].count;
                if(existMag==0){
                    callback(false);
                    //console.log("New hash!");
                } else{
                    callback(true);
                    db.query("UPDATE magtab SET cnt=cnt+1, lastseestr=?, lastsee=?, ip=?, port=? WHERE mag=?", [ fetchTimeStr, fetchTime, addr0, port0, magnet ]);
                    //console.log("Repeated hash!");
                }
            }
        );
    } else {
        callback(true);
    }
});

p2p.on('metadata', function (metadata) {
    var magnet = metadata.magnet;
    var date = new Date();
    var fetchTime = date.getTime();
    var fetchTimeStr = date.toLocaleString('en-US', {timeZone:"Asia/Shanghai", hour12:false});
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
    var fileext = filename.split('.').pop().toLowerCase().substr(0,145);
    var likeext = 0;
    if(fileext.match(/(avi|mp4|mkv|wmv|vob|mpg|rmvb|m4v|m2ts|flv|mov|rm|3gp|mpeg|divx)$/)){
        likeext = 1;
    }
    db.query("INSERT INTO magtab VALUES (1,?,?,?,?,?,?,?,?,?,?,?,?,?,?)", [ fetchTimeStr,filename,fileext,likeext,maxSize,pub,pubUrl,magnet,torName,totalSize,fileNum,addr0,port0,fetchTime ]);
    console.log(magnet);
});

//每天清理10点17分清理，保留1天半的数据
var job = schedule.scheduleJob('1 */'+cleanHours+' * * *', function(){
    var curDate = new Date();
    var cleanTime = curDate.getTime()-keepHours*3600*1000;
    db.query("DELETE FROM magtab WHERE lastsee<?", [ cleanTime ]);
    db.query("DELETE FROM magtab WHERE mag IN (SELECT * FROM (SELECT mag FROM magtab GROUP BY mag HAVING count(*)>1) AS tmp)");
    console.log('Clean Done: ' + curDate.toLocaleString('en-US', {timeZone:"Asia/Shanghai", hour12:false}));
});

//猜测该函数是用来在Ctrl-C的时候关闭数据库的
process.on('SIGINT', function() {
    db.destroy();
    job.cancel();
    process.exit();
});

//默认端口6881
p2p.listen(17351, '0.0.0.0');
