const fs = require('fs/promises');
const path = require('path');
const { Level } = require('level');
const crypto = require('crypto');
const { Worker, isMainThread, parentPort, workerData } = require('worker_threads');
const { performance } = require('perf_hooks');

class StorageBenchmark {
    constructor(config) {
        this.config = {
            numFiles: 25000,
            minFileSize: 5240,
            maxFileSize: 5420400,
            numWorkers: 4,
            concurrentOps: 20,
            basePath: '/tmp/bench',
            ...config
        };

        this.results = {
            leveldb: { write: [], read: [] },
            fs: { write: [], read: [] }
        };

        this.leveldbPath = path.join(this.config.basePath, 'leveldb');
        this.fsPath = path.join(this.config.basePath, 'fs');
    }

    async init() {
        await fs.mkdir(this.config.basePath, { recursive: true });
        await fs.mkdir(this.fsPath, { recursive: true });
        await this.cleanup();
    }

    async cleanup() {
        try {
            await fs.rm(this.fsPath, { recursive: true, force: true });
            await fs.rm(this.leveldbPath, { recursive: true, force: true });
        } catch (err) {
            console.error('Cleanup error:', err);
        }
        await fs.mkdir(this.fsPath, { recursive: true });
        await fs.mkdir(this.leveldbPath, { recursive: true });
    }

    async runWorkers(testType, storageType) {
        const workersCount = this.config.numWorkers;
        const opsPerWorker = Math.ceil(this.config.numFiles / workersCount);

        const workers = Array(workersCount).fill().map((_, index) => {
            return new Promise((resolve, reject) => {
                const worker = new Worker(__filename, {
                    workerData: {
                        testType,
                        storageType,
                        workerId: index,
                        startIndex: index * opsPerWorker,
                        count: Math.min(opsPerWorker, this.config.numFiles - (index * opsPerWorker)),
                        config: this.config
                    }
                });

                worker.on('message', resolve);
                worker.on('error', reject);
                worker.on('exit', (code) => {
                    if (code !== 0) reject(new Error(`Worker stopped with exit code ${code}`));
                });
            });
        });

        return (await Promise.all(workers)).flat();
    }

    async runBenchmark() {
        console.log('Starting benchmark with configuration:', this.config);
        await this.init();

        console.log('\nRunning LevelDB tests...');
        this.results.leveldb.write = await this.runWorkers('write', 'leveldb');
        this.results.leveldb.read = await this.runWorkers('read', 'leveldb');

        console.log('\nRunning FileSystem tests...');
        this.results.fs.write = await this.runWorkers('write', 'fs');
        this.results.fs.read = await this.runWorkers('read', 'fs');

        this.printResults();
    }

    calculateStats(times) {
        times.sort((a, b) => a - b);
        const sum = times.reduce((a, b) => a + b, 0);
        const avg = sum / times.length;
        const median = times[Math.floor(times.length / 2)];
        const p95 = times[Math.floor(times.length * 0.95)];
        const p99 = times[Math.floor(times.length * 0.99)];
        const throughput = this.config.numFiles / sum;

        return {
            avg: avg * 1000,
            median: median * 1000,
            p95: p95 * 1000,
            p99: p99 * 1000,
            throughput
        };
    }

    printResults() {
        console.log('\nBenchmark Results:');
        console.log('=================');

        ['leveldb', 'fs'].forEach(storage => {
            ['write', 'read'].forEach(operation => {
                const stats = this.calculateStats(this.results[storage][operation]);
                console.log(`\n${storage.toUpperCase()} ${operation.toUpperCase()}:`);
                console.log(`Average time: ${stats.avg.toFixed(2)} ms`);
                console.log(`Median time: ${stats.median.toFixed(2)} ms`);
                console.log(`95th percentile: ${stats.p95.toFixed(2)} ms`);
                console.log(`99th percentile: ${stats.p99.toFixed(2)} ms`);
                console.log(`Throughput: ${stats.throughput.toFixed(2)} ops/sec`);
            });
        });
    }
}

async function runWorkerTest({ testType, storageType, workerId, startIndex, count, config }) {
    const results = [];
    let db;

    try {
        if (storageType === 'leveldb') {
            // Создаем отдельную базу для каждого воркера
            const workerDbPath = path.join(config.basePath, 'leveldb', `worker-${workerId}`);
            await fs.mkdir(workerDbPath, { recursive: true });
            
            db = new Level(workerDbPath);
            await db.open();
        }

        const batchSize = config.concurrentOps;
        const batches = Math.ceil(count / batchSize);

        for (let batch = 0; batch < batches; batch++) {
            const batchStart = batch * batchSize;
            const batchCount = Math.min(batchSize, count - batchStart);
            const operations = [];

            for (let i = 0; i < batchCount; i++) {
                const index = startIndex + batchStart + i;
                const fileSize = config.minFileSize + Math.random() * (config.maxFileSize - config.minFileSize);
                const data = crypto.randomBytes(config.fileSize);

                if (testType === 'write') {
                    if (storageType === 'leveldb') {
                        operations.push(db.put(`key_${index}`, data));
                    } else {
                        const filePath = path.join(config.basePath, 'fs', `file_${workerId}_${index}`);
                        operations.push(fs.writeFile(filePath, data));
                    }
                } else {
                    if (storageType === 'leveldb') {
                        operations.push(db.get(`key_${index}`));
                    } else {
                        const filePath = path.join(config.basePath, 'fs', `file_${workerId}_${index}`);
                        operations.push(fs.readFile(filePath));
                    }
                }
            }

            const startTime = performance.now();
            await Promise.all(operations);
            const endTime = performance.now();
            results.push((endTime - startTime) / 1000);
        }

        if (db) {
            await db.close();
        }

        return results;
    } catch (error) {
        if (db) {
            try {
                await db.close();
            } catch (closeError) {
                console.error('Error closing database:', closeError);
            }
        }
        throw error;
    }
}

if (!isMainThread) {
    runWorkerTest(workerData).then(
        result => parentPort.postMessage(result),
        error => {
            console.error('Worker error:', error);
            process.exit(1);
        }
    );
}

if (isMainThread) {
    const benchmark = new StorageBenchmark({
        numFiles: 10000,
        fileSize: 1024,
        numWorkers: 4,
        concurrentOps: 20,
        basePath: path.join(process.cwd(), 'bench-data')  // Используем текущую директорию
    });

    benchmark.runBenchmark().catch(console.error);
}
