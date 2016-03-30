var http = require('http');
var async = require('async');
var url = require('url');

var dataInterface = {
    host: '103.249.254.194',
    port: 3217
};

var types = [
    "dspreq", // dspReq bidderId yyyy-MM-dd dsp请求数
    "dspbid", // dspbid bidderId yyyy-MM-dd dsp参与竞价
    "dspwin", // dspwin bidderId yyyy-MM-dd dsp竞价成功
    "dspview", // dspview bidderId yyyy-MM-dd dsp广告展现数
    "dspclick", // dspclick bidderId yyyy-MM-dd	 dsp 广告点击数
    "dspmoney", // dspmoney  bidderId  yyyy-MM-dd dsp 消耗金额
    "sspreq", // sspreq sspId yyyy-MM-dd ssp 请求数
    "sspresp", // sspresp  sspId  yyyy-MM-dd		ssp 成功响应数
    "sspview", // sspview  sspId  yyyy-MM-dd	ssp 展示数
    "sspclick", // sspclick  sspId  yyyy-MM-dd		ssp 点击数
    "sspmoney", // sspmoney  sspId yyyy-MM-dd		ssp 收益金额
    "pubreq", // pubreq	pubId yyyy-MM-dd		媒体广告请求数
    "pubview", // pubview pubId yyyy-MM-dd		媒体广告展现数
    "pubclick", // pubclick pubId yyyy-MM-dd		媒体广告点击数
    "dspreqdev", // dspdev bidderId yyyy-MM-dd   发给 dsp 的请求中 PC 和 Moblie 的数量
    "sspreqdev", // sspdev bidderId yyyy-MM-dd   ssp 发起的请求中 PC和Moblie 的数量
    "sspsize", // sspsize sspId yyyy-MM-dd      ssp 广告请求的各个 size 类型的数量
    "dspreqsrc" // dspreqsrc bidderId yyyy-MM-dd    查询发给 dsp 的请求来自于那些 ssp. 请求数是多少
];

var getTypeParam = function(type, id, date) {
    date = getYYYYMMDD(date);
    if (type) {
        switch (type) {
            case "dspreq":
                var path = dataInterface.host + ':' + dataInterface.port + '/' + date + '/_count?pretty';
                var data = { query: { prefix: { dsp: id } } };
                return {
                    path: path,
                    data: data
                };
            case "dspbid":
                var path = dataInterface.host + ':' + dataInterface.port + '/' + date + '/_count?pretty';
                var data = { query: { term: { dsp: id + ' set' } } };
                return {
                    path: path,
                    data: data
                };
            case "dspwin":
                var path = dataInterface.host + ':' + dataInterface.port + '/' + date + '/_count?pretty';
                var data = { query: { term: { res: 'success ' + id } } };
                return {
                    path: path,
                    data: data
                };
            case "dspview":
                var path = dataInterface.host + ':' + dataInterface.port + '/_count?pretty';
                var d1 = addOneDay(date);
                var data = { query: { bool: { filter: [{ term: { res: 'success ' + id } }, { range: { viewTime: { gte: date + '+08', lt: d1 + '+08', format: 'yyyy-MM-ddZZ' } } }] } } };
                return {
                    path: path,
                    data: data
                };
            case "dspclick":
                var path = dataInterface.host + ':' + dataInterface.port + '/_count?pretty';
                var d1 = addOneDay(date);
                var data = { query: { bool: { filter: [{ term: { res: 'success ' + id } }, { range: { clickTime: { gte: date + '+08', lt: d1 + '+08', format: 'yyyy-MM-ddZZ' } } }] } } };
                return {
                    path: path,
                    data: data
                };
            case "dspmoney":
                var path = dataInterface.host + ':' + dataInterface.port + '/_search?pretty';
                var d1 = addOneDay(date);
                var data = { query: { bool: { filter: [{ term: { res: 'success ' + id } }, { range: { viewTime: { gte: date + '+08', lt: d1 + '+08', format: 'yyyy-MM-ddZZ' } } }] } }, size: 0, aggs: { dspmoney: { sum: { field: 'dspPrice' } } } };
                return {
                    path: path,
                    data: data
                };
            case "sspreq":
                var path = dataInterface.host + ':' + dataInterface.port + '/' + date + '/_count?pretty';
                var data = { query: { term: { ssp: id } } };
                return {
                    path: path,
                    data: data
                };
            case "sspresp":
                var path = dataInterface.host + ':' + dataInterface.port + '/_count?pretty';
                var data = { query: { bool: { filter: [{ term: { ssp: id } }, { prefix: { res: 'success' } }] } } };
                return {
                    path: path,
                    data: data
                };
            case "sspview":
                var path = dataInterface.host + ':' + dataInterface.port + '/_count?pretty';
                var d1 = addOneDay(date);
                var data = { query: { bool: { filter: [{ term: { ssp: id } }, { range: { viewTime: { gte: date + '+08', lt: d1 + '+08', format: 'yyyy-MM-ddZZ' } } }] } } };
                return {
                    path: path,
                    data: data
                };
            case "sspclick":
                var path = dataInterface.host + ':' + dataInterface.port + '/_count?pretty';
                var d1 = addOneDay(date);
                var data = { query: { bool: { filter: [{ term: { ssp: id } }, { range: { clickTime: { gte: date + '+08', lt: d1 + '+08', format: 'yyyy-MM-ddZZ' } } }] } } };
                return {
                    path: path,
                    data: data
                };
            case "sspmoney":
                var path = dataInterface.host + ':' + dataInterface.port + '/_search?pretty';
                var d1 = addOneDay(date);
                var data = { query: { bool: { filter: [{ term: { ssp: id } }, { range: { viewTime: { gte: date + '+08', lt: d1 + '+08', format: 'yyyy-MM-ddZZ' } } }] } }, size: 0, aggs: { sspmoney: { sum: { field: 'sspPrice' } } } };
                return {
                    path: path,
                    data: data
                };
            case "pubreq":
                var path = dataInterface.host + ':' + dataInterface.port + '/' + date + '/_count?pretty';
                var data = { query: { term: { pub: id } } };
                return {
                    path: path,
                    data: data
                };
            case "pubview":
                var path = dataInterface.host + ':' + dataInterface.port + '/_count?pretty';
                var d1 = addOneDay(date);
                var data = { query: { bool: { filter: [{ term: { pub: id } }, { range: { viewTime: { gte: date + '+08', lt: d1 + '+08', format: 'yyyy-MM-ddZZ' } } }] } } };
                return {
                    path: path,
                    data: data
                };
            case "pubclick":
                var path = dataInterface.host + ':' + dataInterface.port + '/_count?pretty';
                var d1 = addOneDay(date);
                var data = { query: { bool: { filter: [{ term: { pub: id } }, { range: { clickTime: { gte: date + '+08', lt: d1 + '+08', format: 'yyyy-MM-ddZZ' } } }] } } };
                return {
                    path: path,
                    data: data
                };
            case "dspreqdev":
                var path = dataInterface.host + ':' + dataInterface.port + '/' + date + '/_search?pretty';
                var data = { query: { prefix: { dsp: id } }, size: 0, aggs: { devs: { terms: { field: 'size' } } } };
                return {
                    path: path,
                    data: data
                };
            case "sspreqdev":
                var path = dataInterface.host + ':' + dataInterface.port + '/' + date + '/_search?pretty';
                var data = { query: { term: { ssp: id } }, size: 0, aggs: { devs: { terms: { field: 'size' } } } };
                return {
                    path: path,
                    data: data
                };
            case "sspsize":
                var path = dataInterface.host + ':' + dataInterface.port + '/' + date + '/_search?pretty';
                var data = { query: { term: { ssp: id } }, size: 0, aggs: { impSizes: { terms: { field: 'size' } } } };
                return {
                    path: path,
                    data: data
                };
            case "dspreqsrc":
                var path = dataInterface.host + ':' + dataInterface.port + '/' + date + '/_search?pretty';
                var data = { query: { prefix: { dsp: id } }, size: 0, aggs: { reqSrc: { terms: { field: 'ssp' } } } };
                return {
                    path: path,
                    data: data
                };
            default:
                return null;
        }
    }
};

var addOneDay = function(date) {
    date = date instanceof Date ? date : new Date(date || '');
    date.setDate(date.getDate() + 1);
    return getYYYYMMDD(date);
}

var getYYYYMMDD = function(date, split) {
    date = date instanceof Date ? date : new Date(date || '');
    return [date.getFullYear(), ('00' + (date.getMonth() + 1)).slice(-2), ('00' + date.getDate()).slice(-2)].join(split || '-');
};

var getData = function(path, data, method, callback) {
    dataStr = JSON.stringify(data);
    var buf = new Buffer(dataStr, 'utf8');
    var str = '',
        req = null;
    var options = {
        host: path.hostname || 'localhost',
        port: path.port || 3217,
        path: path.path || '',
        method: method || 'POST',
        headers: {
            "Content-Type": "application/x-www-form-urlencoded",
            "Content-Length": buf.length
        }
    };

    req = http.request(options, function(res) {
        str = '';
        res.setEncoding('utf8');
        res.on('data', function(chunk) {
            str += chunk;
        });
        res.on('end', function() {
            callback(str);
        });
    });
    req.on('error', function(e) {
        console.log('problem with request: ' + e.message);
    });

    req.write(dataStr);
    req.end();
};

module.exports = {
    getData: getData,
    getTypeParam: getTypeParam
};

/* API
    getData(path, data, method, callback)
    getTypeParam(type, id, date)
*/

/* test
async.series({
	getData: function (done) {
		var receData = null;
		var obj = getTypeParam('dspmoney', '56811785c30ab406000ead2c', '2016-03-25');
		var path = 'http://' + obj.path;
		path = url.parse(path);
		getData(path, obj.data, '', function (data) {
			receData = data;
			console.log(receData);
			done(null, data);
		});
	},
	test: function (done) {
		console.log('success');
		done(null, 'success');
	}
}, function (err, results) {
	console.log(results);
});
*/