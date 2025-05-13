'use strict';
const nconf = require.main.require('nconf');
const winston = require.main.require('winston');
const { Kafka } = require('kafkajs');
const AWS = require('aws-sdk');
const fs = require('fs');
const path = require('path');
const { v4: uuidv4 } = require('uuid');
const meta = require.main.require('./src/meta');
const controllers = require('./lib/controllers');
const routeHelpers = require.main.require('./src/routes/helpers');
const plugin = {};

// Get configuration from environment variables or use defaults
const getConfig = () => {
  return {
    // Kafka configuration
    kafkaClientId: process.env.KAFKA_CLIENT_ID || 'my-app',
    kafkaBrokers: process.env.KAFKA_BROKERS ? process.env.KAFKA_BROKERS.split(',') : ['localhost:9092'],
    
    // S3/Ozone configuration
    s3Endpoint: process.env.S3_ENDPOINT || 'http://localhost:9878',
    s3AccessKeyId: process.env.S3_ACCESS_KEY_ID || 'admin',
    s3SecretAccessKey: process.env.S3_SECRET_ACCESS_KEY || 'admin',
    s3BucketName: process.env.S3_BUCKET_NAME || 'nodebb-images',
    s3PublicEndpoint: process.env.S3_PUBLIC_ENDPOINT || 'http://localhost:9878',
    exposedPythonProxyEndpoint: process.env.EXPOSED_PYTHON_PROXY_ENDPOINT || 'http://localhost:6969',
    // Topic names for Kafka
    kafkaPostsTopic: process.env.KAFKA_POSTS_TOPIC || 'nodebb-posts',
    kafkaImagesTopic: process.env.KAFKA_IMAGES_TOPIC || 'nodebb-images'
  };
};

plugin.init = async (params) => {
  const { router /* , middleware , controllers */ } = params;
  // Settings saved in the plugin settings can be retrieved via settings methods
  const { setting1, setting2, s3_bucket, s3_endpoint } = await meta.settings.get('kafka');
  
  if (setting1) {
    console.log(setting2);
  }
  
  const config = getConfig();
  
  // Initialize Kafka
  const kafka = new Kafka({
    clientId: config.kafkaClientId,
    brokers: config.kafkaBrokers,
  });
  plugin.kafka_producer = kafka.producer();
  await plugin.kafka_producer.connect();
  
  // Initialize S3 client for Ozone
  plugin.s3 = new AWS.S3({
    endpoint: s3_endpoint || config.s3Endpoint,
    s3ForcePathStyle: true,
    accessKeyId: config.s3AccessKeyId,
    secretAccessKey: config.s3SecretAccessKey,
    signatureVersion: 'v4'
  });
  plugin.config = config
  // Set bucket name
  plugin.bucket = s3_bucket || config.s3BucketName;
  plugin.publicEndpoint = config.s3PublicEndpoint;
  
  // Ensure the bucket exists (create if not)
  try {
    await plugin.ensureBucketExists();
  } catch (err) {
    winston.error('Error ensuring bucket exists:', err);
  }
};

/**
 * We create two routes for every view. One API call, and the actual route itself.
 * Use the `setupPageRoute` helper and NodeBB will take care of everything for you.
 *
 * Other helpers include `setupAdminPageRoute` and `setupAPIRoute`
 */

/**
 * If you wish to add routes to NodeBB's RESTful API, listen to the `static:api.routes` hook.
 * Define your routes similarly to above, and allow core to handle the response via the
 * built-in helpers.formatApiResponse() method.
 */
// IMPORTANT: Keep the method name as handelPosts to match plugin.json
plugin.handelPosts = async function (post) {
  const config = getConfig();
  
  await plugin.kafka_producer.send({
    topic: config.kafkaPostsTopic,
    messages: [{
      key: String(post.post.pid),
      value: JSON.stringify(post)
    }]
  });
};

// Method to ensure bucket exists
plugin.ensureBucketExists = async function() {
  try {
    // Check if bucket exists
    await plugin.s3.headBucket({ Bucket: plugin.bucket }).promise();
    winston.info(`Bucket ${plugin.bucket} already exists`);
  } catch (err) {
    if (err.code === 'NotFound' || err.code === 'NoSuchBucket') {
      // Create the bucket if it doesn't exist
      winston.info(`Creating bucket ${plugin.bucket}...`);
      await plugin.s3.createBucket({
        Bucket: plugin.bucket
      }).promise();
    } else {
      throw err;
    }
  }
};

// Method to upload file to S3/Ozone
plugin.uploadToS3 = async function(filePath, fileName, folder) {
  try {
    winston.info(`Starting S3 upload for ${filePath} to bucket ${plugin.bucket}`);
    const fileContent = fs.readFileSync(filePath);
    
    // Generate unique filename
    const ext = path.extname(fileName);
    const uniqueFileName = `${uuidv4()}${ext}`;
    
    const s3Key = folder ? `${folder}/${uniqueFileName}` : uniqueFileName;
    
    // Try to detect content type from file extension
    const contentType = {
      '.jpg': 'image/jpeg',
      '.jpeg': 'image/jpeg',
      '.png': 'image/png',
      '.gif': 'image/gif',
      '.webp': 'image/webp',
      '.svg': 'image/svg+xml',
      '.bmp': 'image/bmp'
    }[ext.toLowerCase()] || 'application/octet-stream';
    
    const params = {
      Bucket: plugin.bucket,
      Key: s3Key,
      Body: fileContent,
      ContentType: contentType,
      ACL: 'public-read' // Make sure the file is publicly readable
    };
    
    winston.info(`S3 upload parameters: ${JSON.stringify({
      Bucket: plugin.bucket,
      Key: s3Key,
      ContentType: contentType
    })}`);
    
    const uploadResult = await plugin.s3.upload(params).promise();
    winston.info(`S3 upload successful. Location: ${uploadResult.Location}`);
     
    
    // Return upload info
    return {
      url: plugin.config.exposedPythonProxyEndpoint + '/'+plugin.bucket+'/'+s3Key,
      path: s3Key,
      name: uniqueFileName
    };
  } catch (err) {
    winston.error(`Error uploading to S3: ${err.message}`, err);
    throw err;
  }
};

// IMPORTANT: Keep the method name as handelUploads to match plugin.json
plugin.handelUploads = async function (data) {
  const config = getConfig();

  winston.info(`handelUploads called with data: ${JSON.stringify({
    name: data.image.name,
    path: data.image.path
  })}`);
  
  const filename = data.image.name || 'upload';
  
  try {
    // Upload to S3/Ozone instead of saving locally
    const upload = await plugin.uploadToS3(data.image.path, filename, data.folder);
    
    // Send complete info including S3 upload details to Kafka AFTER the upload
    await plugin.kafka_producer.send({
      topic: config.kafkaImagesTopic,
      messages: [{
        key: String(upload.path),
        value: JSON.stringify({
          ...data,
          s3: {
            url: upload.url,
            path: upload.path,
            bucket: plugin.bucket
          }
        })
      }]
    });
    
    winston.info('Image uploaded to S3 successfully', {
      name: upload.name,
      url: upload.url,
      path: upload.path
    });
    
    return {
      url: upload.url,
      path: upload.path,
      name: upload.name
    };
  } catch (err) {
    winston.error(`Error in handelUploads: ${err.message}`, err);
    // If there's an error, let's still try to use the local file as a fallback
    return {
      url: nconf.get('relative_path') + data.image.url,
      path: data.image.path,
      name: data.image.name
    };
  }
};

module.exports = plugin;
