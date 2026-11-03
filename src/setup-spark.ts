import * as core from '@actions/core';
import * as tc from '@actions/tool-cache';
import * as fs from 'fs';
import * as crypto from 'crypto';
import axios, {isAxiosError} from 'axios';

async function validateSubscription() {
  const eventPath = process.env.GITHUB_EVENT_PATH;
  let repoPrivate: boolean | undefined;

  if (eventPath && fs.existsSync(eventPath)) {
    const eventData = JSON.parse(fs.readFileSync(eventPath, 'utf8'));
    repoPrivate = eventData?.repository?.private;
  }

  const upstream = 'vemonet/setup-spark';
  const action = process.env.GITHUB_ACTION_REPOSITORY;
  const docsUrl = 'https://docs.stepsecurity.io/actions/stepsecurity-maintained-actions';

  core.info('');
  core.info('\u001b[1;36mStepSecurity Maintained Action\u001b[0m');
  core.info(`Secure drop-in replacement for ${upstream}`);
  if (repoPrivate === false) core.info('\u001b[32m\u2713 Free for public repositories\u001b[0m');
  core.info(`\u001b[36mLearn more:\u001b[0m ${docsUrl}`);
  core.info('');

  if (repoPrivate === false) return;

  const serverUrl = process.env.GITHUB_SERVER_URL || 'https://github.com';
  const body: Record<string, string> = {action: action || ''};
  if (serverUrl !== 'https://github.com') body['ghes_server'] = serverUrl;
  try {
    await axios.post(
      `https://agent.api.stepsecurity.io/v1/github/${process.env.GITHUB_REPOSITORY}/actions/maintained-actions-subscription`,
      body,
      {timeout: 3000}
    );
  } catch (error) {
    if (isAxiosError(error) && error.response?.status === 403) {
      core.error(`\u001b[1;31mThis action requires a StepSecurity subscription for private repositories.\u001b[0m`);
      core.error(`\u001b[31mLearn how to enable a subscription: ${docsUrl}\u001b[0m`);
      process.exit(1);
    }
    core.info('Timeout or API not reachable. Continuing to next step.');
  }
}

// See docs to create gh action: https://docs.github.com/en/actions/creating-actions/creating-a-javascript-action
const log = (msg: string) => {
  core.info(`${new Date().toLocaleTimeString('fr-FR')} - ${msg}`);
};

async function run() {
  try {
    await validateSubscription();
    const sparkVersion = core.getInput('spark-version');
    const hadoopVersion = core.getInput('hadoop-version');
    const scalaVersion = core.getInput('scala-version');
    const py4jVersion = core.getInput('py4j-version');
    const xmx = core.getInput('xmx');
    const xms = core.getInput('xms');
    const logLevel = core.getInput('log-level');
    let sparkUrl = core.getInput('spark-url');

    // Try to write to the parent folder of the workflow workspace
    const workspaceFolder = process.env.GITHUB_WORKSPACE || '/home/runner/work';
    let installFolder = core.getInput('install-folder') || workspaceFolder.split('/').slice(0, -1).join('/');
    try {
      fs.accessSync(installFolder, fs.constants.R_OK);
    } catch (error) {
      if (core.getInput('install-folder')) throw new Error(`The install folder ${installFolder} is not writable`);
      log(`Using $GITHUB_WORKSPACE to store Spark (${installFolder} not writable)`);
      installFolder = workspaceFolder;
    }
    log(`Spark will be installed to ${installFolder}`);

    const scalaBit = scalaVersion ? `-scala${scalaVersion}` : '';
    let sparkHome = `${installFolder}/spark-${sparkVersion}-bin-hadoop${hadoopVersion}${scalaBit}`;

    const cachedSpark = tc.find('spark', sparkVersion);
    if (cachedSpark) {
      log(`Using Spark from cache ${cachedSpark}`);
      sparkHome = cachedSpark;
    } else if (sparkUrl) {
      // URL provided directly by user
      await download(sparkUrl, installFolder);
    } else {
      // If URL not provided directly, we try to download from official recommended https://spark.apache.org/downloads.html
      sparkUrl = `https://dlcdn.apache.org/spark/spark-${sparkVersion}/spark-${sparkVersion}-bin-hadoop${hadoopVersion}${scalaBit}.tgz`;
      try {
        await download(sparkUrl, installFolder);
      } catch (error) {
        log(`Faster recommended download URL not available, downloading from Apache Archives.`);
        sparkUrl = `https://archive.apache.org/dist/spark/spark-${sparkVersion}/spark-${sparkVersion}-bin-hadoop${hadoopVersion}${scalaBit}.tgz`;
        await download(sparkUrl, installFolder);
      }
    }

    if (!fs.existsSync(`${sparkHome}/bin/spark-submit`)) {
      throw new Error(`The Spark binary downloaded from ${sparkUrl} could not be found in ${sparkHome}`);
    }

    log(`Spark binary downloaded, setting up environment variables and cache`);
    const SPARK_OPTS = `--driver-java-options=-Xms${xms} --driver-java-options=-Xmx${xmx} --driver-java-options=-Dlog4j.logLevel=${logLevel}`;
    const PYTHONPATH = `${sparkHome}/python:${sparkHome}/python/lib/py4j-${py4jVersion}-src.zip`;
    const PYSPARK_PYTHON = 'python';

    // Set environment variables in the workflow
    core.exportVariable('SPARK_HOME', sparkHome);
    core.exportVariable('HADOOP_VERSION', hadoopVersion);
    core.exportVariable('APACHE_SPARK_VERSION', sparkVersion);
    core.exportVariable('PYSPARK_PYTHON', PYSPARK_PYTHON);
    core.exportVariable('PYSPARK_DRIVER_PYTHON', PYSPARK_PYTHON);
    core.exportVariable('PYTHONPATH', PYTHONPATH);
    core.exportVariable('SPARK_OPTS', SPARK_OPTS);

    // Add Spark to path and cache it
    core.addPath(`${sparkHome}/bin`);
    await tc.cacheDir(sparkHome, 'spark', sparkVersion);

    core.setOutput('spark-version', sparkVersion);
  } catch (error) {
    log(
      `Issue installing Spark: check if the Spark version and Hadoop versions you are using are part of the ones proposed on the Spark download page at https://spark.apache.org/downloads.html`
    );
    const err = <Error>error;
    core.error(err);
    core.setFailed(err.message);
  }
}

// Helper function to download and unzip spark binary, verifying checksum if available
async function download(url: string, installFolder: string) {
  log(`Downloading Spark binary from ${url} to ${installFolder}`);
  const zipPath = await tc.downloadTool(url);
  await verifyChecksumIfAvailable(url, zipPath);
  await tc.extractTar(zipPath, installFolder);
}

async function verifyChecksumIfAvailable(url: string, filePath: string) {
  const checksumUrl = `${url}.sha512`;
  let checksumPath: string;
  try {
    checksumPath = await tc.downloadTool(checksumUrl);
  } catch {
    core.warning(
      `⚠️ WARNING: Could not retrieve checksum file from ${checksumUrl}. ` +
        `The integrity of the downloaded Spark archive has NOT been verified. ` +
        `Proceeding is a security risk — ensure the download source is trusted.`
    );
    return;
  }
  const checksumContent = fs.readFileSync(checksumPath, 'utf8').trim();
  // Apache sha512 files are either plain hex or "hash  filename"
  const expected = checksumContent.split(/\s+/)[0].toLowerCase();
  const actual = crypto.createHash('sha512').update(fs.readFileSync(filePath)).digest('hex');
  if (actual !== expected) {
    core.warning(
      `⚠️ SECURITY WARNING: Checksum verification FAILED for ${url}. ` +
        `Expected: ${expected} — Got: ${actual}. ` +
        `The downloaded archive may be corrupted or tampered with. Proceeding anyway, but this is highly unsafe.`
    );
    return;
  }
  log(`Checksum verified successfully for ${url}`);
}

run();
