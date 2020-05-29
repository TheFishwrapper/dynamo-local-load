/*
 * Copyright 2020 Zane Littrell
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
const config = require("./config.json");
const AWS = require("aws-sdk");
const db = new AWS.DynamoDB({
  region: "localhost",
  endpoint: "http://localhost:8000"
});
const s3 = new AWS.S3({
  region: "us-east-1"
});

const BUCKET = process.env.BUCKET || config.bucket;
const STAGE = process.env.STAGE || "dev";

s3.listObjectsV2({ Bucket: BUCKET }).promise()
.then(data => {
  const contents = data.Contents;
  let keys = [];
  for (let i = 0; i < contents.length; i++) {
    const key = contents[i].Key;
    if (key.endsWith(`-${STAGE}.json`)) {
      keys.push(key);
    }
  }
  const promises = keys.map(key => {
    const params = {
      Bucket: BUCKET,
      Key: key
    };
    return s3.getObject(params).promise();
  });
  // Add file names to the front of promises array
  promises.unshift(Promise.resolve(keys));
  return Promise.all(promises);
}).then(data => {
  const filenames = data[0];
  let promises = [];
  for (let i = 1; i < data.length; i++) {
    const filename = filenames[i - 1];
    let table = filename.replace(".json", "");
    const buf = data[i].Body;
    const ar = JSON.parse(buf.toString());
    for (let j = 0; j < ar.length; j++) {
      const params = {
        TableName: table,
        Item: ar[j]
      };
      promises.push(db.putItem(params).promise());
    }
  }
  return Promise.all(promises);
}).then(data => {
  console.log("Loaded tables into local DynamoDB");
}).catch(error => console.error(error));
