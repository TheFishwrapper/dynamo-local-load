#!/usr/bin/env node
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
const fs = require("fs");
const path = require("path");
const AWS = require("aws-sdk");
const db = new AWS.DynamoDB({
  region: "localhost",
  endpoint: "http://localhost:8000"
});
const s3 = new AWS.S3({
  region: "us-east-1"
});

const CONFIG_FILE = ".loadrc";
const FILE_ENCODING = "utf8";

/**
 * Main function that is called when the program is run.
 */
function main() {
  try {
    const configPath = path.join(__dirname, CONFIG_FILE);
    const data = fs.readFileSync(configPath, FILE_ENCODING);
    const configMap = parseConfig(data);
    console.log('Got config ' + JSON.stringify(configMap));
    (async() => await loadBackups(configMap["bucket"], configMap["stage"]))();
    console.log("Loaded tables into local DynamoDB");
  } catch (e) {
    console.error("Error: " + e);
  }
}

/**
 * Parses the config file. Each attribute is on a new line, where the key is in
 * front of a colon and the value follow the colon.
 *
 * @param data String contents of the config file.
 *
 * @return Object of the config attributes.
 */
function parseConfig(data) {
  let configMap = {};
  const lines = data.split(/\r?\n/);
  for (const line of lines) {
    const splitLine = line.split(":");
    configMap[splitLine[0]] = splitLine[1];
  }
  return configMap;
}

/**
 * Loads the backup files from the given bucket that are of the given stage.
 *
 * @param String name of S3 bucket that contains the backups.
 * @param String name of the stage of the backups (dev, production, etc.).
 *
 * @throws Error if there is an error with S3 or DynamoDB.
 */
async function loadBackups(bucket, stage) {
  try {
    const data = await s3.listObjectsV2({ Bucket: bucket }).promise();
    const keys = data.Contents.filter(file => file.Key.endsWith(`-${stage}.json`));
    const backupPromises = keys.map(key => {
      const params = {
        Bucket: bucket,
        Key: key.Key
      };
      return s3.getObject(params).promise();
    });
    const backups = await Promise.all(backupPromises);
    let dynamoPromises = [];
    for (let i = 0; i < keys.length; i++) {
      const table = keys[i].Key.replace(".json", "");
      const ar = JSON.parse(backups[i].Body.toString());
      for (let j = 0; j < ar.length; j++) {
        const params = {
          TableName: table,
          Item: ar[j]
        };
        dynamoPromises.push(db.putItem(params).promise());
      }
    }
    await Promise.all(dynamoPromises);
  } catch (e) {
    throw e;
  }
}

main();
