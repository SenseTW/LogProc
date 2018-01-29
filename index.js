const config = require('config')
const async = require('async');

const nginx_access_log_format = '$remote_addr - $remote_user [$time_local] ' +
                                '"$request_method $request_uri $request_protocol" $status $body_bytes_sent ' +
                                '"$http_referer" "$http_user_agent" ' +
                                'rt=$request_time uct="$upstream_connect_time" uht="$upstream_header_time" urt="$upstream_response_time"';
const nginx_error_log_format = '$time_local [$error_level] $pid#$tid: $cid $message';
const nginx_access_parser = require('deformat')(nginx_access_log_format);
const nginx_error_parser = require('deformat')(nginx_error_log_format);

const pubsub_config = config.get('pubsub');
const loggly_options = config.get('loggly');

const loggly = require('loggly');
var log_client = loggly.createClient(loggly_options);

const PubSub = require('@google-cloud/pubsub');
const pubsub = new PubSub(pubsub_config);
const subscriptionName = pubsub_config.subscriptionName;

var Queue = require('better-queue');

const msgHandler = msg => {
  if (msg.data) {
    var data = Buffer.from(msg.data, 'base64').toString();
    q.push(data, (err, result) => {
      msg.ack();
    });
  } else { 
    console.log(msg);
    msg.ack();
  }
}

const errorHandler = error => {
  console.error(`ERROR: ${error}`);
};

const pullMessage = (callback) => {
  subscription.on('message', msgHandler);
  subscription.on('error', errorHandler);
};

process.stdin.resume();//so the program will not close instantly

function exitHandler(options, err) {
  subscription.removeListener('message', msgHandler);

  q.on('drain', () => {
    console.log("App Closed.");
    process.exit();
  });    

  process.exit();
}

//do something when app is closing
process.on('exit', exitHandler.bind(null,{cleanup:true}));

//catches ctrl+c event
process.on('SIGINT', exitHandler.bind(null, {exit:true}));

// catches "kill pid" (for example: nodemon restart)
process.on('SIGUSR1', exitHandler.bind(null, {exit:true}));
process.on('SIGUSR2', exitHandler.bind(null, {exit:true}));

//catches uncaught exceptions
process.on('uncaughtException', exitHandler.bind(null, {exit:true}));

const newMsgHandler = (msgs, nmh_callback) => {
  let nginxMsg = [];
  let appMsg = [];
  msgs.forEach( (raw_elmt, index, arr) => {
    if (raw_elmt.length < 2) return;

    var elmt = {}
    try {
      elmt = JSON.parse(raw_elmt);
    } catch(e) {
      console.log("error: ", e);
      console.log(raw_elmt);
      return;
    }

    if (!elmt.logName) {
      console.log(elmt);
    }
    var logCat = elmt.logName.split("/").pop();
    if (logCat.endsWith("-web")) {
      nginxMsg.push(elmt);
    } else {
      appMsg.push(elmt);
    }
  });

  async.auto({
    nginxLogProcess: (callback) => {
      nginxLogHandler(nginxMsg, (results) => {
        callback(null, results);
      });
    },
    appLogProcess: (callback) => {
      callback(null);
    },
    afterLogProcess: ['nginxLogProcess', 'appLogProcess', (results, callback) => {
      nmh_callback(null, results);
    }]
  });
}

const nginxLogHandler = (logs, nlh_callback) => {
  async.auto({
    parseLogs: callback => {
      var content_objs = [];
      logs.forEach( (elmt, index, arr) => {
        if (!elmt) return;
        if (elmt["severity"] === "INFO") {
          elmt["parsedPayload"] = nginx_access_parser.exec(elmt["textPayload"].replace("\n",""));
        } else {
          elmt["parsedPayload"] = nginx_error_parser.exec(elmt["textPayload"].replace("\n","")); 
        }
        content_objs.push(elmt);
      });
      callback(null, content_objs);
    },
    transformLogMsg: ['parseLogs', (results, callback) => {
      var reformedLogs = transformNginxLogsToLoggly(results.parseLogs);
      callback(null, reformedLogs);
    }],
    sendToLoggly: ['transformLogMsg', (results, callback) => {
      sendLogToLoggly(results.transformLogMsg, ['nginx'], nlh_callback);
    }]
  }, (err, results) => {
    nlh_callback(results.make_msg);
  });
}

const sendLogToLoggly = (logs, tags, callback) => {
  log_client.log(logs, tags, (error, result) => {
    if (error) {
      console.log('error:', error);
      callback(error);
    } else {
      console.log(`${logs.length} logs has been sent to loggly.`);
      callback(null, result);
    }
  });
}

const transformNginxLogsToLoggly = (origin_logs) => {
  var new_logs = [];
  for(var i=0;i<origin_logs.length;++i) {
    var log_info = {
      severity: origin_logs[i].severity,
      timestamp: origin_logs[i].timestamp
    };
    
    if (origin_logs[i]["error_parse_payload"]) {
      log_info["error_parse_payload"] = origin_logs[i]["error_parse_payload"];
      new_logs.push(log_info);
    } else {
      var log = Object.assign({}, origin_logs[i]["parsedPayload"], origin_logs[i].resource.labels, log_info);
      new_logs.push(log);
    }
  }
  return new_logs;
}


var q = new Queue(newMsgHandler, { batchSize: 100, batchDelayTimeout: 10000 }); //10s
var subscription = pubsub.subscription(subscriptionName);

pullMessage( (msg, obj) => {
  console.log(msg);
});
