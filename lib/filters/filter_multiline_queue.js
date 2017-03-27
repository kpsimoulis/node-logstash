var base_filter_buffer = require('../lib/base_filter_buffer'),
  util = require('util'),
  logger = require('log4node'),
  AWS = require('aws-sdk');

function FilterMultilineQueue() {
  base_filter_buffer.BaseFilterBuffer.call(this);
  this.mergeConfig({
    name: 'MultilineQueue',
    optional_params: ['max_delay', 'max_size'],
    default_values: {
      max_delay: 5000,
      max_size: 240000,
      counter: 0,
      size: 0,
      monitor_lines: 0,
    },
    start_hook: this.start,
  });
}

function getBinarySize(string) {
  return Buffer.byteLength(string, 'utf8');
}

util.inherits(FilterMultilineQueue, base_filter_buffer.BaseFilterBuffer);

FilterMultilineQueue.prototype.setInterval = function(delay) {
  var func = function() {
    var now = (new Date()).getTime();
    var to_be_deleted = [];
    for (var key in this.storage) {
      if (now - this.storage[key].last > delay) {
        logger.info('reset counter and send data by interval');
        this.counter = 0;
        this.size = 0;
        this.sendMessage(key, this.storage[key].current);
        to_be_deleted.push(key);
      }
    }
    to_be_deleted.forEach(function(key) {
      delete this.storage[key];
    }.bind(this));
  }.bind(this);
  this.interval_id = setInterval(func, delay);
};

FilterMultilineQueue.prototype.setMonitor = function() {
  var func = function() {

    var value = Math.ceil((this.monitor_lines + this.counter) / 60);

    var cloudwatch = new AWS.CloudWatch({ region: '{{ region }}', apiVersion: '2010-08-01'});

    var params = {
      MetricData: [ /* required */
        {
          MetricName: 'httpd-qps', /* required */
          Dimensions: [
            {
              Name: 'InstanceId', /* required */
              Value: '{{ ansible_ec2_instance_id }}' /* required */
            },
            /* more items */
          ],
          Timestamp: new Date,
          Unit: 'Count/Second',
          Value: value
        },
        /* more items */
      ],
      Namespace: 'System/Linux' /* required */
    };
    cloudwatch.putMetricData(params, function(err, data) {
      if (err) logger.error(err); // an error occurred
      else     logger.info(data);           // successful response
    });


    logger.info('current lines per minute: ' + value);
    this.monitor_lines = 0;
  }.bind(this);
  this.monitor_interval_id = setInterval(func, 60000);
};

FilterMultilineQueue.prototype.start = function(callback) {
  logger.info('Initialized filter_multiline_queue with max_size: ' + this.max_size + ', delay: ' + (this.max_delay));
  this.setInterval(this.max_delay);
  this.setMonitor();
  callback();
};

FilterMultilineQueue.prototype.process = function(data) {
  var key = this.computeKey(data);
  this.size += getBinarySize(data.message);

  logger.debug('counter is ' + this.counter + ' and size is ' + this.size);

  /**
   * We stop at a safe threshold e.g. 240000
   */
  if (this.size >= this.max_size) {
    logger.info('sending ' + this.counter+' lines to SQS' + ' with length ' + this.size);
    this.sendIfNeeded(key);
    this.monitor_lines += this.counter;
    this.counter = 0;
    this.size = 0;
  }
  this.store(key, data);
  this.counter++;
};

exports.create = function() {
  return new FilterMultilineQueue();
};
