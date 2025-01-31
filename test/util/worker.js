'use strict';

const workerThreads = require('worker_threads');
const { fork } = require('child_process');

class ForkError extends Error {
  constructor(message) {
    super(message);

    this.name = 'ForkError';
  }
}

exports.runWorker = (filename, workerData, timeout = 1500) => {
  return new Promise((resolve, reject) => {
    let timeoutId = null;
    let workerError = null;

    const worker = new workerThreads.Worker(filename, {
      workerData,
      stdout: true,
      stderr: true
    });

    if (timeout > 0) {
      timeoutId = setTimeout(() => {
        workerError = new Error(`Worker timed out after ${timeout}ms`);
        worker.terminate();
      }, timeout);
    }

    worker.on('error', (err) => {
      workerError = err;
    });

    // Handle worker exit
    worker.on('exit', (code) => {
      if (timeoutId)
        clearTimeout(timeoutId);

      if (code !== 0) {
        const error = workerError || new Error(`Worker stopped with exit code ${code}`);
        reject(error);
        return;
      }

      if (workerError) {
        reject(workerError);
        return;
      }

      resolve();
    });
  });
};

exports.runForked = (filename, data, timeout = 1500) => {
  return new Promise((resolve, reject) => {
    let timeoutId = null;
    let processError = null;
    const stderrs = [];

    const child = fork(filename, {
      stdio: ['pipe', 'pipe', 'pipe', 'ipc'],
      env: {
        ...process.env,
        WORKER_DATA: JSON.stringify(data) // Pass data through env since we can't use workerData
      }
    });

    child.stderr.on('data', data => stderrs.push(data));

    if (timeout > 0) {
      timeoutId = setTimeout(() => {
        processError = new Error(`Process timed out after ${timeout}ms`);

        // SIGTERM first, then SIGKILL after a grace period
        child.kill('SIGTERM');

        setTimeout(() => {
          if (!child.killed)
            child.kill('SIGKILL');
        }, 200);
      }, timeout);
    }

    child.on('error', (err) => {
      processError = err;
    });

    // Handle process exit
    child.on('exit', (code, signal) => {
      if (timeoutId)
        clearTimeout(timeoutId);

      if (signal) {
        let error = processError;

        if (!error)
          error = new Error(`Process killed with signal ${signal}`);

        reject(error);
        return;
      }

      if (code !== 0) {
        let error = processError;

        if (!error && stderrs.length) {
          const str = stderrs.join('');
          error = new ForkError(str.split('\n')[0]);
          error.stack = str.split('\n').slice(1).join('\n');
        }

        if (!error)
          error = new Error(`Process stopped with exit code ${code}`);

        reject(error);
        return;
      }

      if (processError) {
        reject(processError);
        return;
      }

      resolve();
    });
  });
};
