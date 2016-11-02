'use strict';

var path = require('path'),
	mime = require('mime'),
	AWS = require('aws-sdk'),
	through2 = require('through2'),
	url = require('s3-url');

var encodings = {
	'gz': 'gzip',
	'xz': 'xz'
};

/**
 * Create a stream which can be piped vinyl files that get uploaded to S3.
 * Once the files have finished being uploaded, they will be re-emitted so
 * that other streams can re-use them later in the pipeline.
 *
 * Vinyl files that have the `awsOptions` property set automatically have those
 * options passed through to S3.
 *
 * @param {String|Object} root Destination to write to.
 * @param {Object} options Stream options.
 * @param {AWS.S3} options.s3 S3 instance.
 * @param {String} options.base Prefix for all keys.
 * @param {Object} options.awsOptions Settings for AWS.
 * @param {String} options.awsOptions.Bucket Destination bucket to write to.
 * @returns {Stream.Transform} Stream.
 */
module.exports = function createWriteStream(root, options) {

	options = Object.assign({
		s3: new AWS.S3(),
		awsOptions: { }
	}, options);

	var s3 = options.s3,
		awsOptions = options.awsOptions,
		prefix;

	if (!s3 || typeof s3.putObject !== 'function') {
		throw new TypeError();
	}

	Object.assign(awsOptions, typeof root === 'string' ? url.urlToOptions(root) : root);

	if (!awsOptions.hasOwnProperty('Bucket')) {
		throw new TypeError();
	}

	prefix = awsOptions.Key ? awsOptions.Key + '/' : '';

	return through2.obj(function writeStream(file, encoding, callback) {

		function done(err) {
			callback(err, file);
		}

		var name = path.basename(file.path),
			contentType = file.contentType,
			contentEncoding = file.contentEncoding;

		// If the file extensions match a known encoding and there isn't an
		// encoding set for the file then use the ones we guessed at.
		if (!contentEncoding) {
			if (!name.split('.').some(function checkPart(ext) {
				if (encodings[ext]) {
					contentEncoding = [ encodings[ext] ];
					return true;
				}
			})) {
				contentEncoding = [];
			}

		}

		// If there is a content encoding set then strip off any extensions
		// that are from the encoding processes to get the extension for the
		// "real" content type.
		if (contentEncoding.length > 0 && !contentType) {
			var toks = name.split('.');
			for (var i = toks.length -1; i >= 0; i--) {
				var encoding = toks[i];
				if (!contentEncoding.includes(encodings[toks[i]])) {
					contentType = mime.lookup(encoding);
					break;
				}
			}
		}

		// If we still haven't got a content type, then make it our best guess
		// from the file name.
		if (!contentType) {
			contentType = mime.lookup(name);
		}

		var fileOptions = Object.assign({}, awsOptions, {
			Key: prefix + file.relative.replace(/\\/g, '/'),
			ContentType: contentType,
			ContentEncoding: contentEncoding.join(',')
		}, file.awsOptions);

		if (!file.isNull()) {
			// Use the new S3 streaming upload API; for more info see
			// https://github.com/aws/aws-sdk-js/pull/427
			var params = Object.assign(fileOptions, {
				Body: file.contents
			});
			s3.upload(params, { queueSize: options.queueSize, partSize: options.partSize },  done);
		} else {
			done();
		}
	});
};
